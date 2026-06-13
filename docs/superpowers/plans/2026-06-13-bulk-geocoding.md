# Bulk Geocoding (Census Batch Service) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Census *batch* geocoding so large volumes of addresses are geocoded efficiently, auto-routing by volume (1 pending → single GET; 2+ → chunked CSV upload of ≤1,000 addresses per callout) through the existing durable message queue.

**Architecture:** A new `GeocodeDispatcher` (Queueable) replaces the trigger's inline message creation: it queries `Pending` `Geocode_Request__c` records and either builds one single-address GET message or chunks them into batch messages that POST a multipart CSV to the Census `addressbatch` endpoint. `MessageQueueService` gains generic `multipart/form-data` support; a new `GeocodeBatchResponseHandler` parses the CSV response back onto each request. The engine claims at most one "heavy" (`Is_Batch__c`) message per processor execution and chains, to respect Salesforce's 120s cumulative-callout limit.

**Tech Stack:** Salesforce Apex (API 67), SOQL, Queueable/Schedulable Apex, HTTP callouts (multipart/form-data), `sf` CLI for deploy/test against the `devbc-sandbox` org.

**Spec:** `docs/superpowers/specs/2026-06-13-bulk-geocoding-census-batch-design.md`

**Working branch:** `feature/bulk-geocoding` (already created). All `sf` commands target `--target-org devbc-sandbox`.

**Note on the Salesforce TDD loop:** Apex tests run server-side, so the "see it fail" step means *deploy the test before the implementation exists and observe the compile/assertion failure*. Each task writes the test, deploys (fails), implements, redeploys with the test (passes), commits.

---

## File Structure

**New files:**
- `force-app/main/default/classes/CsvUtil.cls` (+ `-meta.xml`) — quoted-CSV line parser.
- `force-app/main/default/classes/GeocodeDispatcher.cls` (+ meta) — volume routing, CSV building, reaper.
- `force-app/main/default/classes/GeocodeBatchResponseHandler.cls` (+ meta) — parse batch CSV → requests.
- `force-app/main/default/classes/BatchGeocodeMock.cls` (+ meta) — test `HttpCalloutMock` returning Census CSV.
- `force-app/main/default/classes/GeocodeBatchTest.cls` (+ meta) — unit tests for the above.
- `force-app/main/default/objects/Message_Queue__c/fields/Is_Batch__c.field-meta.xml` — heavy-message flag.

**Modified files:**
- `force-app/main/default/classes/MessageQueueService.cls` — multipart support, `buildBatchMessage`, heavy-aware claim.
- `force-app/main/default/triggers/GeocodeRequestTrigger.trigger` — enqueue dispatcher instead of inline messages.
- `force-app/main/default/objects/Geocode_Request__c/fields/Geocode_Status__c.field-meta.xml` — add `Queued` value.
- `force-app/main/default/permissionsets/Message_Queue_Admin.permissionset-meta.xml` — FLS for `Is_Batch__c`.
- `force-app/main/default/classes/GeocodeExampleTest.cls` — adapt to async dispatch.

**Unchanged (confirmed):** `MessageQueueProcessor.cls` needs no changes — heavy-aware *claiming* bounds callouts per execution; the processor just processes whatever it is handed and already chains via `claimPending`.

---

## Task 1: Metadata — `Is_Batch__c` field, `Queued` status, permission set FLS

**Files:**
- Create: `force-app/main/default/objects/Message_Queue__c/fields/Is_Batch__c.field-meta.xml`
- Modify: `force-app/main/default/objects/Geocode_Request__c/fields/Geocode_Status__c.field-meta.xml`
- Modify: `force-app/main/default/permissionsets/Message_Queue_Admin.permissionset-meta.xml`

- [ ] **Step 1: Create the `Is_Batch__c` checkbox field**

`force-app/main/default/objects/Message_Queue__c/fields/Is_Batch__c.field-meta.xml`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<CustomField xmlns="http://soap.sforce.com/2006/04/metadata">
    <fullName>Is_Batch__c</fullName>
    <label>Is Batch</label>
    <description>True when this message carries a batch of records in one callout (e.g. a Census addressbatch CSV upload). The engine processes at most one batch message per execution.</description>
    <type>Checkbox</type>
    <defaultValue>false</defaultValue>
    <trackHistory>false</trackHistory>
</CustomField>
```

- [ ] **Step 2: Add the `Queued` picklist value to `Geocode_Status__c`**

In `force-app/main/default/objects/Geocode_Request__c/fields/Geocode_Status__c.field-meta.xml`, add a new `<value>` block inside `<valueSetDefinition>` immediately after the `Pending` value (before `Matched`):
```xml
            <value>
                <fullName>Queued</fullName>
                <default>false</default>
                <label>Queued</label>
            </value>
```

- [ ] **Step 3: Grant FLS on `Is_Batch__c` in `Message_Queue_Admin`**

In `force-app/main/default/permissionsets/Message_Queue_Admin.permissionset-meta.xml`, add this `<fieldPermissions>` block immediately after the opening that grants `Status__c` (any position among the field blocks is fine):
```xml
    <fieldPermissions>
        <field>Message_Queue__c.Is_Batch__c</field>
        <readable>true</readable>
        <editable>true</editable>
    </fieldPermissions>
```

- [ ] **Step 4: Deploy the metadata**

Run:
```bash
sf project deploy start --target-org devbc-sandbox --ignore-conflicts \
  --source-dir force-app/main/default/objects/Message_Queue__c/fields/Is_Batch__c.field-meta.xml \
  --source-dir force-app/main/default/objects/Geocode_Request__c/fields/Geocode_Status__c.field-meta.xml \
  --source-dir force-app/main/default/permissionsets/Message_Queue_Admin.permissionset-meta.xml
```
Expected: `Status: Succeeded`, 3 components deployed.

- [ ] **Step 5: Commit**

```bash
git add force-app/main/default/objects/Message_Queue__c/fields/Is_Batch__c.field-meta.xml \
        force-app/main/default/objects/Geocode_Request__c/fields/Geocode_Status__c.field-meta.xml \
        force-app/main/default/permissionsets/Message_Queue_Admin.permissionset-meta.xml
git commit -m "feat: add Is_Batch__c field, Queued geocode status, and FLS for bulk geocoding"
```

---

## Task 2: `CsvUtil.parseLine` — quoted-CSV line parser

**Files:**
- Create: `force-app/main/default/classes/CsvUtil.cls` (+ meta)
- Test: add `csvParser*` methods to `force-app/main/default/classes/GeocodeBatchTest.cls` (created here)

- [ ] **Step 1: Write the failing test**

Create `force-app/main/default/classes/GeocodeBatchTest.cls`:
```apex
/**
 * Unit tests for bulk (Census batch) geocoding: CSV parsing, multipart message
 * construction, the batch response handler, and the dispatcher's volume routing.
 */
@IsTest
private class GeocodeBatchTest {

    // ---- CsvUtil --------------------------------------------------------
    @IsTest
    static void csvParserSplitsPlainFields() {
        List<String> cols = CsvUtil.parseLine('a,b,c');
        System.assertEquals(new List<String>{ 'a', 'b', 'c' }, cols);
    }

    @IsTest
    static void csvParserHandlesQuotedCommas() {
        List<String> cols = CsvUtil.parseLine('id,"1600 PENNSYLVANIA AVE, WASHINGTON, DC","-77.03,38.89"');
        System.assertEquals(3, cols.size());
        System.assertEquals('id', cols[0]);
        System.assertEquals('1600 PENNSYLVANIA AVE, WASHINGTON, DC', cols[1]);
        System.assertEquals('-77.03,38.89', cols[2]);
    }

    @IsTest
    static void csvParserHandlesEmptyAndEscapedQuotes() {
        List<String> cols = CsvUtil.parseLine('a,,"he said ""hi"""');
        System.assertEquals(3, cols.size());
        System.assertEquals('a', cols[0]);
        System.assertEquals('', cols[1]);
        System.assertEquals('he said "hi"', cols[2]);
    }
}
```

Create `force-app/main/default/classes/GeocodeBatchTest.cls-meta.xml`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<ApexClass xmlns="http://soap.sforce.com/2006/04/metadata">
    <apiVersion>67.0</apiVersion>
    <status>Active</status>
</ApexClass>
```

- [ ] **Step 2: Deploy the test to verify it fails**

Run:
```bash
sf project deploy start --target-org devbc-sandbox --ignore-conflicts \
  --source-dir force-app/main/default/classes/GeocodeBatchTest.cls
```
Expected: FAIL — compile error `Invalid type: CsvUtil` (class does not exist yet).

- [ ] **Step 3: Write the implementation**

Create `force-app/main/default/classes/CsvUtil.cls`:
```apex
/**
 * Minimal RFC-4180-ish CSV line parser. Splits one line into fields, honoring
 * double-quoted fields (which may contain commas) and escaped quotes (""). Apex
 * has no built-in CSV parser; the Census batch geocoder returns quoted CSV whose
 * matched-address and coordinate fields contain commas.
 */
public with sharing class CsvUtil {

    public static List<String> parseLine(String line) {
        List<String> result = new List<String>();
        if (line == null) {
            return result;
        }
        Boolean inQuotes = false;
        String current = '';
        Integer len = line.length();
        for (Integer i = 0; i < len; i++) {
            String c = line.substring(i, i + 1);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < len && line.substring(i + 1, i + 2) == '"') {
                        current += '"';   // escaped quote
                        i++;
                    } else {
                        inQuotes = false; // closing quote
                    }
                } else {
                    current += c;
                }
            } else if (c == '"') {
                inQuotes = true;
            } else if (c == ',') {
                result.add(current);
                current = '';
            } else {
                current += c;
            }
        }
        result.add(current);
        return result;
    }
}
```

Create `force-app/main/default/classes/CsvUtil.cls-meta.xml`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<ApexClass xmlns="http://soap.sforce.com/2006/04/metadata">
    <apiVersion>67.0</apiVersion>
    <status>Active</status>
</ApexClass>
```

- [ ] **Step 4: Deploy and run the tests to verify they pass**

Run:
```bash
sf project deploy start --target-org devbc-sandbox --ignore-conflicts \
  --source-dir force-app/main/default/classes/CsvUtil.cls \
  --source-dir force-app/main/default/classes/GeocodeBatchTest.cls \
  --test-level RunSpecifiedTests --tests GeocodeBatchTest
```
Expected: `Status: Succeeded`, 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add force-app/main/default/classes/CsvUtil.cls force-app/main/default/classes/CsvUtil.cls-meta.xml \
        force-app/main/default/classes/GeocodeBatchTest.cls force-app/main/default/classes/GeocodeBatchTest.cls-meta.xml
git commit -m "feat: add CsvUtil quoted-CSV line parser with tests"
```

---

## Task 3: `MessageQueueService` — multipart support + `buildBatchMessage`

**Files:**
- Modify: `force-app/main/default/classes/MessageQueueService.cls`
- Test: `force-app/main/default/classes/GeocodeBatchTest.cls`

- [ ] **Step 1: Write the failing tests**

Add to `GeocodeBatchTest`:
```apex
    // ---- buildBatchMessage + multipart payload --------------------------
    @IsTest
    static void buildBatchMessageProducesMultipartPayload() {
        String csv = '001000000000001AAA,4550 Montgomery Ave,Bethesda,MD,20814';
        Message_Queue__c m = MessageQueueService.buildBatchMessage(csv);

        System.assertEquals('POST', m.HTTP_Method__c);
        System.assertEquals(true, m.Is_Batch__c, 'Batch messages must be flagged');
        System.assertEquals('GeocodeBatchResponseHandler', m.Response_Handler__c);
        System.assert(m.Endpoint__c.contains('addressbatch'), 'Endpoint targets the batch service');
        System.assert(m.Payload__c.contains('addressFile'), 'Payload carries the multipart file field');
        System.assert(m.Payload__c.contains('Public_AR_Current'), 'Payload carries the benchmark');
        System.assert(m.Payload__c.contains('4550 Montgomery Ave'), 'Payload carries the CSV content');
    }

    @IsTest
    static void multipartPayloadRoundTripsThroughParse() {
        String csv = '001000000000001AAA,4550 Montgomery Ave,Bethesda,MD,20814';
        Message_Queue__c m = MessageQueueService.buildBatchMessage(csv);

        MessageQueueService.MessagePayload p = MessageQueueService.parsePayload(m);
        System.assertNotEquals(null, p.multipart, 'multipart block should survive serialize/parse');
        System.assertEquals('addressFile', p.multipart.fileFieldName);
        System.assertEquals(csv, p.multipart.fileContent);
        System.assertEquals('Public_AR_Current', p.multipart.formFields.get('benchmark'));
    }
```

- [ ] **Step 2: Deploy to verify failure**

Run:
```bash
sf project deploy start --target-org devbc-sandbox --ignore-conflicts \
  --source-dir force-app/main/default/classes/GeocodeBatchTest.cls
```
Expected: FAIL — compile error `Method does not exist or incorrect signature: ... buildBatchMessage` and `Variable does not exist: multipart`.

- [ ] **Step 3: Add the `MultipartUpload` inner class and `multipart` field**

In `MessageQueueService.cls`, inside the `MessagePayload` class, add a field after `responseHandler`:
```apex
        public MultipartUpload multipart;      // optional; multipart/form-data file upload
```

Immediately after the `MessagePayload` class closes (before the "Enqueue helpers" comment block), add:
```apex
    /** Describes a single multipart/form-data file upload plus accompanying form fields. */
    public class MultipartUpload {
        public String fileFieldName;            // e.g. "addressFile"
        public String fileName;                 // e.g. "addresses.csv"
        public String fileContent;              // the file body (text)
        public Map<String, String> formFields;  // additional form-data fields, e.g. benchmark
    }
```

- [ ] **Step 4: Map `multipart` in `parsePayload`**

In `parsePayload`, immediately before the `if (String.isBlank(payload.endpoint))` check, add:
```apex
        if (raw.get('multipart') instanceof Map<String, Object>) {
            Map<String, Object> mp = (Map<String, Object>) raw.get('multipart');
            MultipartUpload mu = new MultipartUpload();
            mu.fileFieldName = (String) mp.get('fileFieldName');
            mu.fileName = (String) mp.get('fileName');
            mu.fileContent = (String) mp.get('fileContent');
            if (mp.get('formFields') instanceof Map<String, Object>) {
                mu.formFields = new Map<String, String>();
                Map<String, Object> ff = (Map<String, Object>) mp.get('formFields');
                for (String k : ff.keySet()) {
                    mu.formFields.put(k, String.valueOf(ff.get(k)));
                }
            }
            payload.multipart = mu;
        }
```

- [ ] **Step 5: Add `buildBatchMessage` and the multipart body builder**

In `MessageQueueService.cls`, add a constant near the other constants at the top of the class:
```apex
    @TestVisible private static final String CENSUS_BATCH_ENDPOINT =
        'https://geocoding.geo.census.gov/geocoder/locations/addressbatch';
```

Add these methods in the "Enqueue helpers" section (after the existing `buildMessage` overloads):
```apex
    /**
     * Build (but do not insert) a batch message: a single POST that uploads a CSV
     * of up to ~1,000 addresses to the Census addressbatch geocoder as
     * multipart/form-data. The CSV's first column is the Geocode_Request__c Id,
     * which the response echoes back so results map straight to the records.
     */
    public static Message_Queue__c buildBatchMessage(String csv) {
        MessagePayload payload = new MessagePayload();
        payload.method = 'POST';
        payload.endpoint = CENSUS_BATCH_ENDPOINT;
        payload.timeout = 120000;               // batch geocoding is slow; use the max
        payload.responseHandler = 'GeocodeBatchResponseHandler';

        MultipartUpload mu = new MultipartUpload();
        mu.fileFieldName = 'addressFile';
        mu.fileName = 'addresses.csv';
        mu.fileContent = csv;
        mu.formFields = new Map<String, String>{ 'benchmark' => 'Public_AR_Current' };
        payload.multipart = mu;

        return new Message_Queue__c(
            Payload__c = JSON.serialize(payload, true),
            HTTP_Method__c = 'POST',
            Endpoint__c = payload.endpoint.abbreviate(255),
            Response_Handler__c = 'GeocodeBatchResponseHandler',
            Is_Batch__c = true,
            Status__c = 'Pending'
        );
    }
```

- [ ] **Step 6: Send multipart in `executeCallout`**

In `executeCallout`, replace the body-setting block:
```apex
        String body = bodyToString(payload.body);
        if (String.isNotBlank(body) && payload.method != 'GET' && payload.method != 'DELETE') {
            request.setBody(body);
        }

        return new Http().send(request);
```
with:
```apex
        if (payload.multipart != null) {
            String boundary = '----MessageQueueBoundary7MA4YWxkTrZu0gW';
            request.setHeader('Content-Type', 'multipart/form-data; boundary=' + boundary);
            request.setBodyAsBlob(Blob.valueOf(buildMultipartBody(payload.multipart, boundary)));
        } else {
            String body = bodyToString(payload.body);
            if (String.isNotBlank(body) && payload.method != 'GET' && payload.method != 'DELETE') {
                request.setBody(body);
            }
        }

        return new Http().send(request);
```

Add this private helper in the "Internals" section:
```apex
    /**
     * Assemble a multipart/form-data body as text. The file content here is text
     * (CSV), so a plain string body is sufficient; no base64 byte-alignment is
     * needed (that trick only matters when embedding binary file parts).
     */
    private static String buildMultipartBody(MultipartUpload mu, String boundary) {
        String CRLF = '\r\n';
        String body = '';
        if (mu.formFields != null) {
            for (String name : mu.formFields.keySet()) {
                body += '--' + boundary + CRLF;
                body += 'Content-Disposition: form-data; name="' + name + '"' + CRLF + CRLF;
                body += mu.formFields.get(name) + CRLF;
            }
        }
        body += '--' + boundary + CRLF;
        body += 'Content-Disposition: form-data; name="' + mu.fileFieldName
              + '"; filename="' + mu.fileName + '"' + CRLF;
        body += 'Content-Type: text/csv' + CRLF + CRLF;
        body += (mu.fileContent == null ? '' : mu.fileContent) + CRLF;
        body += '--' + boundary + '--' + CRLF;
        return body;
    }
```

- [ ] **Step 7: Deploy and run tests**

Run:
```bash
sf project deploy start --target-org devbc-sandbox --ignore-conflicts \
  --source-dir force-app/main/default/classes/MessageQueueService.cls \
  --source-dir force-app/main/default/classes/GeocodeBatchTest.cls \
  --test-level RunSpecifiedTests --tests GeocodeBatchTest --tests MessageQueueTest
```
Expected: `Status: Succeeded`; all `GeocodeBatchTest` and `MessageQueueTest` tests pass (confirms no regression in the existing engine tests).

- [ ] **Step 8: Commit**

```bash
git add force-app/main/default/classes/MessageQueueService.cls force-app/main/default/classes/GeocodeBatchTest.cls
git commit -m "feat: add multipart/form-data support and buildBatchMessage to the queue engine"
```

---

## Task 4: Heavy-aware claiming (one batch callout per execution)

**Files:**
- Modify: `force-app/main/default/classes/MessageQueueService.cls`
- Test: `force-app/main/default/classes/GeocodeBatchTest.cls`

- [ ] **Step 1: Write the failing test**

Add to `GeocodeBatchTest`:
```apex
    // ---- Heavy-aware claiming ------------------------------------------
    @IsTest
    static void claimReturnsAtMostOneBatchAndPrefersSingles() {
        // Two batch messages and one single message, all eligible now.
        Message_Queue__c b1 = MessageQueueService.buildBatchMessage('001000000000001AAA,a,b,c,d');
        Message_Queue__c b2 = MessageQueueService.buildBatchMessage('001000000000002AAA,a,b,c,d');
        Message_Queue__c single = MessageQueueService.buildMessage(
            'GET', 'https://example.com', null, null, null, null);
        insert new List<Message_Queue__c>{ b1, b2, single };

        Test.startTest();
        List<Message_Queue__c> first = MessageQueueService.claimPending(100);
        Test.stopTest();

        // Singles are claimed first, in bulk; batch messages are not mixed in.
        System.assertEquals(1, first.size(), 'Single message claimed first, batches deferred');
        System.assertEquals(single.Id, first[0].Id);

        // Next claim returns exactly one batch (heavy work one-at-a-time).
        List<Message_Queue__c> second = MessageQueueService.claimPending(100);
        System.assertEquals(1, second.size(), 'At most one batch message per claim');
        System.assertEquals(true,
            [SELECT Is_Batch__c FROM Message_Queue__c WHERE Id = :second[0].Id].Is_Batch__c);
    }
```

- [ ] **Step 2: Deploy to verify failure**

Run:
```bash
sf project deploy start --target-org devbc-sandbox --ignore-conflicts \
  --source-dir force-app/main/default/classes/GeocodeBatchTest.cls
```
Expected: FAIL — assertion failure (current `claimPending` ignores `Is_Batch__c` and would claim all three at once).

- [ ] **Step 3: Refactor `claimPending` to be heavy-aware**

In `MessageQueueService.cls`, replace the entire current `claimPending` method:
```apex
    public static List<Message_Queue__c> claimPending(Integer limitSize) {
        Datetime now = System.now();
        // SOQL forbids ORDER BY together with FOR UPDATE (lock order is implied by Id).
        // Two-step claim: (1) pick the highest-priority eligible rows in order, then
        // (2) lock those specific rows for the atomic status flip.
        List<Message_Queue__c> candidates = [
            SELECT Id
            FROM Message_Queue__c
            WHERE Status__c IN ('Pending', 'Retry')
              AND (Scheduled_At__c = null OR Scheduled_At__c <= :now)
              AND (Next_Attempt_At__c = null OR Next_Attempt_At__c <= :now)
            ORDER BY Priority__c ASC NULLS LAST, Scheduled_At__c ASC NULLS FIRST, CreatedDate ASC
            LIMIT :limitSize
        ];
        if (candidates.isEmpty()) {
            return new List<Message_Queue__c>();
        }
        Set<Id> candidateIds = new Map<Id, Message_Queue__c>(candidates).keySet();
        List<Message_Queue__c> toClaim = [
            SELECT Id
            FROM Message_Queue__c
            WHERE Id IN :candidateIds
              AND Status__c IN ('Pending', 'Retry')
            FOR UPDATE
        ];
        for (Message_Queue__c m : toClaim) {
            m.Status__c = 'Processing';
        }
        if (!toClaim.isEmpty()) {
            update toClaim;
        }
        return toClaim;
    }
```
with:
```apex
    /**
     * Claim eligible messages for one processor execution. Cheap single-callout
     * messages are claimed in bulk and drained first; "heavy" batch messages
     * (Is_Batch__c = true) are claimed at most one at a time, because Salesforce
     * caps cumulative callout time at 120s per transaction and each batch callout
     * is slow. The processor chains, so successive executions drain the rest.
     */
    public static List<Message_Queue__c> claimPending(Integer limitSize) {
        List<Message_Queue__c> singles = claimEligible(false, limitSize);
        if (!singles.isEmpty()) {
            return singles;
        }
        return claimEligible(true, 1);
    }

    private static List<Message_Queue__c> claimEligible(Boolean isBatch, Integer limitSize) {
        Datetime now = System.now();
        // SOQL forbids ORDER BY together with FOR UPDATE (lock order is implied by Id).
        // Two-step claim: (1) pick the highest-priority eligible rows in order, then
        // (2) lock those specific rows for the atomic status flip.
        List<Message_Queue__c> candidates = [
            SELECT Id
            FROM Message_Queue__c
            WHERE Status__c IN ('Pending', 'Retry')
              AND Is_Batch__c = :isBatch
              AND (Scheduled_At__c = null OR Scheduled_At__c <= :now)
              AND (Next_Attempt_At__c = null OR Next_Attempt_At__c <= :now)
            ORDER BY Priority__c ASC NULLS LAST, Scheduled_At__c ASC NULLS FIRST, CreatedDate ASC
            LIMIT :limitSize
        ];
        if (candidates.isEmpty()) {
            return new List<Message_Queue__c>();
        }
        Set<Id> candidateIds = new Map<Id, Message_Queue__c>(candidates).keySet();
        List<Message_Queue__c> toClaim = [
            SELECT Id
            FROM Message_Queue__c
            WHERE Id IN :candidateIds
              AND Status__c IN ('Pending', 'Retry')
            FOR UPDATE
        ];
        for (Message_Queue__c m : toClaim) {
            m.Status__c = 'Processing';
        }
        if (!toClaim.isEmpty()) {
            update toClaim;
        }
        return toClaim;
    }
```

- [ ] **Step 4: Deploy and run tests**

Run:
```bash
sf project deploy start --target-org devbc-sandbox --ignore-conflicts \
  --source-dir force-app/main/default/classes/MessageQueueService.cls \
  --source-dir force-app/main/default/classes/GeocodeBatchTest.cls \
  --test-level RunSpecifiedTests --tests GeocodeBatchTest --tests MessageQueueTest
```
Expected: `Status: Succeeded`; all tests pass (the existing `claimPendingFlipsToProcessingInPriorityOrder` still passes since those messages are non-batch).

- [ ] **Step 5: Commit**

```bash
git add force-app/main/default/classes/MessageQueueService.cls force-app/main/default/classes/GeocodeBatchTest.cls
git commit -m "feat: claim batch messages one-at-a-time to respect cumulative callout limit"
```

---

## Task 5: `GeocodeBatchResponseHandler` — parse CSV response → requests

**Files:**
- Create: `force-app/main/default/classes/GeocodeBatchResponseHandler.cls` (+ meta)
- Create: `force-app/main/default/classes/BatchGeocodeMock.cls` (+ meta)
- Test: `force-app/main/default/classes/GeocodeBatchTest.cls`

- [ ] **Step 1: Write the failing test and the mock**

Create `force-app/main/default/classes/BatchGeocodeMock.cls`:
```apex
/** Test HttpCalloutMock that returns a fixed status/body for the batch geocoder. */
@IsTest
public class BatchGeocodeMock implements HttpCalloutMock {
    private final Integer statusCode;
    private final String body;
    public HttpRequest lastRequest { get; private set; }

    public BatchGeocodeMock(Integer statusCode, String body) {
        this.statusCode = statusCode;
        this.body = body;
    }

    public HttpResponse respond(HttpRequest request) {
        this.lastRequest = request;
        HttpResponse response = new HttpResponse();
        response.setStatusCode(statusCode);
        response.setStatus(statusCode == 200 ? 'OK' : 'ERR');
        if (body != null) {
            response.setBody(body);
        }
        return response;
    }
}
```
Create `BatchGeocodeMock.cls-meta.xml` (same content as the `GeocodeBatchTest.cls-meta.xml` in Task 2).

Add to `GeocodeBatchTest`:
```apex
    // ---- GeocodeBatchResponseHandler -----------------------------------
    @IsTest
    static void batchHandlerWritesMatchesAndNoMatches() {
        Geocode_Request__c matched = new Geocode_Request__c(
            Street__c = '4550 Montgomery Ave', City__c = 'Bethesda',
            State__c = 'MD', Zip__c = '20814', Geocode_Status__c = 'Queued');
        Geocode_Request__c missing = new Geocode_Request__c(
            Street__c = 'Nowhere', City__c = 'X', State__c = 'XX',
            Zip__c = '00000', Geocode_Status__c = 'Queued');
        insert new List<Geocode_Request__c>{ matched, missing };

        // Census batch CSV: ID, input, match indicator, match type, matched addr, "lon,lat", tiger, side
        String csv =
            '"' + matched.Id + '","4550 Montgomery Ave, Bethesda, MD, 20814","Match","Exact",'
            + '"4550 MONTGOMERY AVE, BETHESDA, MD, 20814","-77.0916,38.9839","123456","L"\n'
            + '"' + missing.Id + '","Nowhere, X, XX, 00000","No_Match"';

        HttpResponse resp = new HttpResponse();
        resp.setStatusCode(200);
        resp.setBody(csv);

        Message_Queue__c msg = MessageQueueService.buildBatchMessage('irrelevant');

        Test.startTest();
        new GeocodeBatchResponseHandler().handle(msg, resp);
        Test.stopTest();

        Map<Id, Geocode_Request__c> byId = new Map<Id, Geocode_Request__c>([
            SELECT Geocode_Status__c, Matched_Address__c,
                   Location__Latitude__s, Location__Longitude__s, Geocoded_At__c
            FROM Geocode_Request__c WHERE Id IN :new List<Id>{ matched.Id, missing.Id }
        ]);

        Geocode_Request__c m = byId.get(matched.Id);
        System.assertEquals('Matched', m.Geocode_Status__c);
        System.assertEquals('4550 MONTGOMERY AVE, BETHESDA, MD, 20814', m.Matched_Address__c);
        System.assert(Math.abs(m.Location__Latitude__s - 38.9839) < 0.0001, 'latitude (y)');
        System.assert(Math.abs(m.Location__Longitude__s - (-77.0916)) < 0.0001, 'longitude (x)');
        System.assertNotEquals(null, m.Geocoded_At__c);

        System.assertEquals('No_Match', byId.get(missing.Id).Geocode_Status__c);
    }

    @IsTest
    static void batchHandlerLeavesProcessingOnNon2xx() {
        Geocode_Request__c r = new Geocode_Request__c(
            Street__c = 'A', City__c = 'B', State__c = 'CC', Zip__c = '11111',
            Geocode_Status__c = 'Queued');
        insert r;
        HttpResponse resp = new HttpResponse();
        resp.setStatusCode(500);
        resp.setBody('error');
        Message_Queue__c msg = MessageQueueService.buildBatchMessage('x');
        msg.Status__c = 'Processing';

        new GeocodeBatchResponseHandler().handle(msg, resp);

        System.assertEquals('Processing', msg.Status__c, 'Non-2xx must leave Processing for retry');
        System.assertEquals('Queued',
            [SELECT Geocode_Status__c FROM Geocode_Request__c WHERE Id = :r.Id].Geocode_Status__c);
    }
```

- [ ] **Step 2: Deploy to verify failure**

Run:
```bash
sf project deploy start --target-org devbc-sandbox --ignore-conflicts \
  --source-dir force-app/main/default/classes/GeocodeBatchTest.cls \
  --source-dir force-app/main/default/classes/BatchGeocodeMock.cls
```
Expected: FAIL — compile error `Invalid type: GeocodeBatchResponseHandler`.

- [ ] **Step 3: Write the handler**

Create `force-app/main/default/classes/GeocodeBatchResponseHandler.cls`:
```apex
/**
 * Response handler for the Census BATCH geocoder. Parses the CSV response and
 * writes results back onto each originating Geocode_Request__c. The CSV's first
 * column is the Geocode_Request__c Id we uploaded, which Census echoes back.
 *
 * Census batch CSV columns (no header), quoted:
 *   ID, Input Address, Match Indicator, Match Type, Matched Address,
 *   Coordinates ("longitude,latitude"), TIGER Line ID, Side
 *
 * On a non-2xx response it leaves message.Status__c as 'Processing' so the queue
 * engine applies its retry/backoff policy to the whole batch.
 */
public with sharing class GeocodeBatchResponseHandler implements MessageQueueResponseHandler {

    public void handle(Message_Queue__c message, HttpResponse response) {
        Integer status = response.getStatusCode();
        if (status < 200 || status >= 300) {
            return; // transport/server error: let the engine retry the batch
        }

        Map<Id, Geocode_Request__c> updates = new Map<Id, Geocode_Request__c>();
        String body = response.getBody();
        if (String.isBlank(body)) {
            return;
        }

        for (String line : body.split('\n')) {
            if (String.isBlank(line)) {
                continue;
            }
            List<String> cols = CsvUtil.parseLine(line.trim());
            if (cols.size() < 3) {
                continue;
            }
            Id reqId;
            try {
                reqId = (Id) cols[0];
            } catch (Exception e) {
                continue; // unparseable id column
            }

            Geocode_Request__c req = new Geocode_Request__c(Id = reqId);
            req.Geocoded_At__c = System.now();

            if (cols[2] == 'Match' && cols.size() >= 6) {
                req.Matched_Address__c = abbreviate(cols[4], 255);
                List<String> coord = cols[5].split(',');
                if (coord.size() == 2) {
                    req.Location__Longitude__s = toDecimal(coord[0]); // x = longitude
                    req.Location__Latitude__s = toDecimal(coord[1]);  // y = latitude
                }
                req.Geocode_Status__c = 'Matched';
            } else {
                // 'No_Match' or 'Tie' both treated as No_Match
                req.Geocode_Status__c = 'No_Match';
            }
            updates.put(reqId, req);
        }

        if (!updates.isEmpty()) {
            update updates.values();
        }
        // 2xx: leave message.Status__c as 'Processing' so the engine marks it Completed.
    }

    private Decimal toDecimal(Object value) {
        try {
            return value == null ? null : Decimal.valueOf(String.valueOf(value).trim());
        } catch (Exception e) {
            return null;
        }
    }

    private String abbreviate(String value, Integer maxLen) {
        if (value == null) {
            return null;
        }
        return value.length() <= maxLen ? value : value.abbreviate(maxLen);
    }
}
```
Create `GeocodeBatchResponseHandler.cls-meta.xml` (same content as the meta in Task 2).

- [ ] **Step 4: Deploy and run tests**

Run:
```bash
sf project deploy start --target-org devbc-sandbox --ignore-conflicts \
  --source-dir force-app/main/default/classes/GeocodeBatchResponseHandler.cls \
  --source-dir force-app/main/default/classes/BatchGeocodeMock.cls \
  --source-dir force-app/main/default/classes/GeocodeBatchTest.cls \
  --test-level RunSpecifiedTests --tests GeocodeBatchTest
```
Expected: `Status: Succeeded`; all tests pass.

- [ ] **Step 5: Commit**

```bash
git add force-app/main/default/classes/GeocodeBatchResponseHandler.cls force-app/main/default/classes/GeocodeBatchResponseHandler.cls-meta.xml \
        force-app/main/default/classes/BatchGeocodeMock.cls force-app/main/default/classes/BatchGeocodeMock.cls-meta.xml \
        force-app/main/default/classes/GeocodeBatchTest.cls
git commit -m "feat: add GeocodeBatchResponseHandler to parse Census batch CSV results"
```

---

## Task 6: `GeocodeDispatcher` — volume routing, CSV building, reaper

**Files:**
- Create: `force-app/main/default/classes/GeocodeDispatcher.cls` (+ meta)
- Test: `force-app/main/default/classes/GeocodeBatchTest.cls`

- [ ] **Step 1: Write the failing tests**

Add to `GeocodeBatchTest`:
```apex
    // ---- GeocodeDispatcher routing -------------------------------------
    private static List<Geocode_Request__c> makeRequests(Integer n) {
        List<Geocode_Request__c> reqs = new List<Geocode_Request__c>();
        for (Integer i = 0; i < n; i++) {
            reqs.add(new Geocode_Request__c(
                Street__c = i + ' Main St', City__c = 'Town', State__c = 'MD',
                Zip__c = '20814', Geocode_Status__c = 'Pending'));
        }
        return reqs;
    }

    @IsTest
    static void dispatchSingleUsesGetPath() {
        // Suppress the trigger's auto-enqueue; dispatch manually for a deterministic test.
        GeocodeDispatcher.dispatchQueued = true;
        insert makeRequests(1);

        Test.startTest();
        GeocodeDispatcher.dispatch();
        Test.stopTest();

        List<Message_Queue__c> msgs = [SELECT Is_Batch__c, HTTP_Method__c, Response_Handler__c FROM Message_Queue__c];
        System.assertEquals(1, msgs.size());
        System.assertEquals(false, msgs[0].Is_Batch__c, 'Single request -> single GET message');
        System.assertEquals('GET', msgs[0].HTTP_Method__c);
        System.assertEquals('CensusGeocodeResponseHandler', msgs[0].Response_Handler__c);
        System.assertEquals(1, [SELECT COUNT() FROM Geocode_Request__c WHERE Geocode_Status__c = 'Queued']);
    }

    @IsTest
    static void dispatchManyChunksIntoBatches() {
        GeocodeDispatcher.dispatchQueued = true;
        insert makeRequests(1500); // -> two batches: 1000 + 500

        Test.startTest();
        GeocodeDispatcher.dispatch();
        Test.stopTest();

        List<Message_Queue__c> batches = [SELECT Id FROM Message_Queue__c WHERE Is_Batch__c = true];
        System.assertEquals(2, batches.size(), '1500 requests -> two batch messages (1000 + 500)');
        System.assertEquals(0, [SELECT COUNT() FROM Message_Queue__c WHERE Is_Batch__c = false]);
        System.assertEquals(1500, [SELECT COUNT() FROM Geocode_Request__c WHERE Geocode_Status__c = 'Queued']);
    }

    @IsTest
    static void dispatchTwoUsesBatchPath() {
        GeocodeDispatcher.dispatchQueued = true;
        insert makeRequests(2);

        Test.startTest();
        GeocodeDispatcher.dispatch();
        Test.stopTest();

        System.assertEquals(1, [SELECT COUNT() FROM Message_Queue__c WHERE Is_Batch__c = true],
            '2 requests -> one batch message');
    }

    @IsTest
    static void reaperResetsStuckQueuedRequestsFromFailedBatch() {
        GeocodeDispatcher.dispatchQueued = true;
        Geocode_Request__c r = makeRequests(1)[0];
        insert r;
        // Simulate a permanently-failed batch whose CSV references this request.
        String csv = r.Id + ',1 Main St,Town,MD,20814';
        Message_Queue__c failed = MessageQueueService.buildBatchMessage(csv);
        failed.Status__c = 'Failed';
        insert failed;
        update new Geocode_Request__c(Id = r.Id, Geocode_Status__c = 'Queued');

        Test.startTest();
        GeocodeDispatcher.dispatch(); // runs the reaper first
        Test.stopTest();

        System.assertEquals('Error',
            [SELECT Geocode_Status__c FROM Geocode_Request__c WHERE Id = :r.Id].Geocode_Status__c,
            'Stuck Queued request from a Failed batch is reaped to Error');
    }
```

- [ ] **Step 2: Deploy to verify failure**

Run:
```bash
sf project deploy start --target-org devbc-sandbox --ignore-conflicts \
  --source-dir force-app/main/default/classes/GeocodeBatchTest.cls
```
Expected: FAIL — compile error `Invalid type: GeocodeDispatcher`.

- [ ] **Step 3: Write the dispatcher**

Create `force-app/main/default/classes/GeocodeDispatcher.cls`:
```apex
/**
 * Routes Pending Geocode_Request__c records onto the message queue by volume:
 *   - exactly 1 Pending  -> a single GET to the Census onelineaddress geocoder
 *                           (richer match metadata), handled by CensusGeocodeResponseHandler.
 *   - 2 or more Pending  -> chunks of up to BATCH_SIZE addresses, each uploaded as a
 *                           CSV to the Census addressbatch geocoder, handled by
 *                           GeocodeBatchResponseHandler.
 *
 * Dispatched requests are flipped to 'Queued' so a later run never re-dispatches them.
 * Enqueued once per transaction by GeocodeRequestTrigger (deduped via dispatchQueued).
 * If more than MAX_PER_RUN are pending, it re-enqueues itself to continue.
 *
 * Also reaps requests left stuck in 'Queued' by a permanently-Failed batch message,
 * resetting them to 'Error'.
 */
public with sharing class GeocodeDispatcher implements Queueable {

    @TestVisible private static final Integer BATCH_SIZE = 1000;
    @TestVisible private static final Integer MAX_PER_RUN = 25000;
    private static final String SINGLE_BASE =
        'https://geocoding.geo.census.gov/geocoder/locations/onelineaddress';

    /** Set true by the trigger once per transaction to dedupe enqueueing. */
    @TestVisible public static Boolean dispatchQueued = false;

    public void execute(QueueableContext context) {
        dispatch();
    }

    public static void dispatch() {
        reapFailedBatches();

        List<Geocode_Request__c> pending = [
            SELECT Id, Street__c, City__c, State__c, Zip__c
            FROM Geocode_Request__c
            WHERE Geocode_Status__c = 'Pending'
            ORDER BY CreatedDate ASC
            LIMIT :MAX_PER_RUN
        ];
        if (pending.isEmpty()) {
            return;
        }

        List<Message_Queue__c> messages = new List<Message_Queue__c>();
        List<Geocode_Request__c> toMark = new List<Geocode_Request__c>();

        if (pending.size() == 1) {
            Geocode_Request__c r = pending[0];
            String endpoint = buildSingleEndpoint(r);
            if (endpoint != null) {
                messages.add(MessageQueueService.buildMessage(
                    'GET', endpoint, null,
                    new Map<String, String>{ 'Accept' => 'application/json' },
                    'CensusGeocodeResponseHandler', r.Id));
                toMark.add(new Geocode_Request__c(Id = r.Id, Geocode_Status__c = 'Queued'));
            }
        } else {
            for (Integer i = 0; i < pending.size(); i += BATCH_SIZE) {
                List<Geocode_Request__c> chunk = new List<Geocode_Request__c>();
                Integer end = Math.min(i + BATCH_SIZE, pending.size());
                for (Integer j = i; j < end; j++) {
                    chunk.add(pending[j]);
                }
                for (Geocode_Request__c r : chunk) {
                    toMark.add(new Geocode_Request__c(Id = r.Id, Geocode_Status__c = 'Queued'));
                }
                messages.add(MessageQueueService.buildBatchMessage(buildCsv(chunk)));
            }
        }

        if (!messages.isEmpty()) {
            insert messages;
            update toMark;
        }

        // If we filled the per-run ceiling, more may remain: continue on a fresh job.
        if (pending.size() == MAX_PER_RUN && !Test.isRunningTest()
                && Limits.getQueueableJobs() < Limits.getLimitQueueableJobs()) {
            System.enqueueJob(new GeocodeDispatcher());
        }
    }

    private static String buildSingleEndpoint(Geocode_Request__c r) {
        List<String> parts = addressParts(r);
        if (parts.isEmpty()) {
            return null;
        }
        return SINGLE_BASE
            + '?address=' + EncodingUtil.urlEncode(String.join(parts, ', '), 'UTF-8')
            + '&benchmark=Public_AR_Current'
            + '&format=json';
    }

    private static List<String> addressParts(Geocode_Request__c r) {
        List<String> parts = new List<String>();
        if (String.isNotBlank(r.Street__c)) { parts.add(r.Street__c); }
        if (String.isNotBlank(r.City__c))   { parts.add(r.City__c); }
        if (String.isNotBlank(r.State__c))  { parts.add(r.State__c); }
        if (String.isNotBlank(r.Zip__c))    { parts.add(r.Zip__c); }
        return parts;
    }

    /** CSV with no header: Id, Street, City, State, ZIP (one row per request). */
    private static String buildCsv(List<Geocode_Request__c> reqs) {
        List<String> lines = new List<String>();
        for (Geocode_Request__c r : reqs) {
            lines.add(String.join(new List<String>{
                r.Id, cell(r.Street__c), cell(r.City__c), cell(r.State__c), cell(r.Zip__c)
            }, ','));
        }
        return String.join(lines, '\n');
    }

    private static String cell(String v) {
        if (v == null) {
            return '';
        }
        if (v.contains(',') || v.contains('"') || v.contains('\n')) {
            return '"' + v.replace('"', '""') + '"';
        }
        return v;
    }

    /**
     * Reset requests stuck in 'Queued' because their batch message permanently
     * Failed. Only 'Queued' rows are touched, so this is idempotent and never
     * clobbers a request an operator manually reset to Pending.
     */
    private static void reapFailedBatches() {
        Set<Id> suspectIds = new Set<Id>();
        for (Message_Queue__c m : [
            SELECT Payload__c FROM Message_Queue__c
            WHERE Is_Batch__c = true AND Status__c = 'Failed'
            LIMIT 200
        ]) {
            suspectIds.addAll(requestIdsFromPayload(m.Payload__c));
        }
        if (suspectIds.isEmpty()) {
            return;
        }
        List<Geocode_Request__c> toError = new List<Geocode_Request__c>();
        for (Geocode_Request__c r : [
            SELECT Id FROM Geocode_Request__c
            WHERE Id IN :suspectIds AND Geocode_Status__c = 'Queued'
        ]) {
            toError.add(new Geocode_Request__c(Id = r.Id, Geocode_Status__c = 'Error'));
        }
        if (!toError.isEmpty()) {
            update toError;
        }
    }

    private static List<Id> requestIdsFromPayload(String payloadJson) {
        List<Id> ids = new List<Id>();
        try {
            Map<String, Object> raw = (Map<String, Object>) JSON.deserializeUntyped(payloadJson);
            Map<String, Object> mp = (Map<String, Object>) raw.get('multipart');
            String csv = mp == null ? null : (String) mp.get('fileContent');
            if (csv == null) {
                return ids;
            }
            for (String line : csv.split('\n')) {
                if (String.isBlank(line)) {
                    continue;
                }
                List<String> cols = CsvUtil.parseLine(line);
                try {
                    ids.add((Id) cols[0]);
                } catch (Exception ignore) {
                    // skip non-id rows
                }
            }
        } catch (Exception e) {
            // unparseable payload -> nothing to reap
        }
        return ids;
    }
}
```
Create `GeocodeDispatcher.cls-meta.xml` (same content as the meta in Task 2).

- [ ] **Step 4: Deploy and run tests**

Run:
```bash
sf project deploy start --target-org devbc-sandbox --ignore-conflicts \
  --source-dir force-app/main/default/classes/GeocodeDispatcher.cls \
  --source-dir force-app/main/default/classes/GeocodeBatchTest.cls \
  --test-level RunSpecifiedTests --tests GeocodeBatchTest
```
Expected: `Status: Succeeded`; all `GeocodeBatchTest` tests pass.

- [ ] **Step 5: Commit**

```bash
git add force-app/main/default/classes/GeocodeDispatcher.cls force-app/main/default/classes/GeocodeDispatcher.cls-meta.xml \
        force-app/main/default/classes/GeocodeBatchTest.cls
git commit -m "feat: add GeocodeDispatcher with volume routing, CSV building, and reaper"
```

---

## Task 7: Trigger change + adapt `GeocodeExampleTest`

**Files:**
- Modify: `force-app/main/default/triggers/GeocodeRequestTrigger.trigger`
- Modify: `force-app/main/default/classes/GeocodeExampleTest.cls`

- [ ] **Step 1: Update the existing test to expect async dispatch**

In `GeocodeExampleTest.cls`, replace the `insertRequest` helper:
```apex
    private static Geocode_Request__c insertRequest() {
        Geocode_Request__c req = new Geocode_Request__c(
            Street__c = '1600 Pennsylvania Ave NW',
            City__c = 'Washington',
            State__c = 'DC',
            Zip__c = '20500'
        );
        insert req; // after-insert trigger enqueues the Census message
        return req;
    }
```
with:
```apex
    private static Geocode_Request__c insertRequest() {
        // Suppress the trigger's async auto-enqueue and dispatch synchronously so
        // the test observes the resulting message deterministically.
        GeocodeDispatcher.dispatchQueued = true;
        Geocode_Request__c req = new Geocode_Request__c(
            Street__c = '1600 Pennsylvania Ave NW',
            City__c = 'Washington',
            State__c = 'DC',
            Zip__c = '20500'
        );
        insert req;
        GeocodeDispatcher.dispatch(); // 1 pending -> single GET message
        return req;
    }
```

- [ ] **Step 2: Deploy the test to verify it fails**

Run:
```bash
sf project deploy start --target-org devbc-sandbox --ignore-conflicts \
  --source-dir force-app/main/default/classes/GeocodeExampleTest.cls \
  --test-level RunSpecifiedTests --tests GeocodeExampleTest
```
Expected: FAIL — `triggerEnqueuesCensusCallout` still passes via the manual dispatch, but the trigger ALSO creates a message at insert (old behavior), so two messages now exist and `System.assertEquals(1, msgs.size())` fails. This confirms the trigger must stop creating messages inline.

- [ ] **Step 3: Update the trigger to enqueue the dispatcher**

Replace the entire body of `force-app/main/default/triggers/GeocodeRequestTrigger.trigger`:
```apex
/**
 * EXAMPLE producer trigger for the Census geocoding use case.
 *
 * On insert of Pending Geocode_Request__c records it enqueues a single
 * GeocodeDispatcher job (deduped per transaction). The dispatcher decides, by
 * volume, whether to issue individual GET callouts or batch CSV uploads to the
 * Census geocoder, then the scheduled processor performs the callout(s).
 *
 * Routing lives in GeocodeDispatcher (not here) because a bulk insert arrives in
 * 200-record trigger chunks and cannot see the total pending volume.
 */
trigger GeocodeRequestTrigger on Geocode_Request__c (after insert) {

    if (GeocodeDispatcher.dispatchQueued) {
        return; // already enqueued (or suppressed by a test) in this transaction
    }

    for (Geocode_Request__c req : Trigger.new) {
        if (req.Geocode_Status__c == 'Pending') {
            GeocodeDispatcher.dispatchQueued = true;
            System.enqueueJob(new GeocodeDispatcher());
            break;
        }
    }
}
```

- [ ] **Step 4: Deploy and run both geocode test classes**

Run:
```bash
sf project deploy start --target-org devbc-sandbox --ignore-conflicts \
  --source-dir force-app/main/default/triggers/GeocodeRequestTrigger.trigger \
  --source-dir force-app/main/default/classes/GeocodeExampleTest.cls \
  --test-level RunSpecifiedTests --tests GeocodeExampleTest --tests GeocodeBatchTest
```
Expected: `Status: Succeeded`; all tests pass.

- [ ] **Step 5: Commit**

```bash
git add force-app/main/default/triggers/GeocodeRequestTrigger.trigger force-app/main/default/classes/GeocodeExampleTest.cls
git commit -m "feat: route geocode requests through GeocodeDispatcher instead of inline trigger messages"
```

---

## Task 8: Full deploy + run the entire test suite

**Files:** none (verification only)

- [ ] **Step 1: Deploy everything and run all local tests**

Run:
```bash
sf project deploy start --target-org devbc-sandbox --ignore-conflicts \
  --test-level RunLocalTests
```
Expected: `Status: Succeeded`; all tests across `MessageQueueTest`, `GeocodeExampleTest`, and `GeocodeBatchTest` pass with no failures.

- [ ] **Step 2: If any test fails, fix and re-run**

Read the failure, fix the offending class, redeploy with `--test-level RunSpecifiedTests --tests <class>` until green, then re-run Step 1. Do not proceed until RunLocalTests is fully green.

- [ ] **Step 3: Commit any fixes**

```bash
git add -A
git commit -m "test: ensure full local test suite passes for bulk geocoding"
```
(Skip if there was nothing to fix.)

---

## Task 9: Live end-to-end validation against the real Census batch endpoint

**Files:** none (manual validation; uses a temporary anonymous-Apex script)

- [ ] **Step 1: Create the validation script**

Create a temporary file `bulk_e2e.apex` (not committed):
```apex
// Insert several real addresses so the dispatcher uses the BATCH path (2+),
// then dispatch + drive a tick to make the real Census batch callout.
GeocodeDispatcher.dispatchQueued = true; // dispatch manually below
List<Geocode_Request__c> reqs = new List<Geocode_Request__c>{
    new Geocode_Request__c(Street__c='4550 Montgomery Ave', City__c='Bethesda', State__c='MD', Zip__c='20814', Geocode_Status__c='Pending'),
    new Geocode_Request__c(Street__c='1600 Pennsylvania Ave NW', City__c='Washington', State__c='DC', Zip__c='20500', Geocode_Status__c='Pending'),
    new Geocode_Request__c(Street__c='1 Infinite Loop', City__c='Cupertino', State__c='CA', Zip__c='95014', Geocode_Status__c='Pending')
};
insert reqs;
GeocodeDispatcher.dispatch();
new MessageQueueScheduler().execute(null); // claims the batch message, enqueues processor
System.debug('>>> DISPATCHED ' + reqs.size() + ' requests');
```

- [ ] **Step 2: Run it**

Run:
```bash
sf apex run --target-org devbc-sandbox --file bulk_e2e.apex
```
Expected: compiles and runs; debug shows `>>> DISPATCHED 3 requests`. The processor Queueable then runs asynchronously and makes the real callout.

- [ ] **Step 3: Poll for results (wait a few seconds, then query)**

Run:
```bash
sf data query --target-org devbc-sandbox --query "SELECT Geocode_Status__c, Matched_Address__c, Location__Latitude__s, Location__Longitude__s FROM Geocode_Request__c WHERE CreatedDate = TODAY ORDER BY CreatedDate DESC LIMIT 10"
```
Expected: the three requests show `Geocode_Status__c = Matched` with populated `Matched_Address__c` and coordinates. Also verify the batch message completed:
```bash
sf data query --target-org devbc-sandbox --query "SELECT Status__c, Is_Batch__c, Response_Status_Code__c FROM Message_Queue__c WHERE Is_Batch__c = true ORDER BY CreatedDate DESC LIMIT 5"
```
Expected: `Status__c = Completed`, `Response_Status_Code__c = 200`.

- [ ] **Step 4: If the live call fails, debug the multipart body**

The most likely failure is a malformed multipart body. Retrieve the batch message's `Response_Body__c` and `Error_Message__c`:
```bash
sf data query --target-org devbc-sandbox --query "SELECT Status__c, Response_Status_Code__c, Error_Message__c, Response_Body__c FROM Message_Queue__c WHERE Is_Batch__c = true ORDER BY CreatedDate DESC LIMIT 1"
```
Inspect the Census error, adjust `buildMultipartBody` in `MessageQueueService.cls` (boundary, headers, or CRLF handling), redeploy, and re-run from Step 2.

- [ ] **Step 5: Clean up the temp script**

```bash
rm -f bulk_e2e.apex
```

---

## Task 10: Push the branch and open the PR

**Files:** none

- [ ] **Step 1: Push the branch**

```bash
git push -u origin feature/bulk-geocoding
```

- [ ] **Step 2: Open the PR**

Open a PR from `feature/bulk-geocoding` into `main`, titled "Add bulk geocoding via Census batch service", summarizing: auto-route by volume, 1,000/callout, heavy-aware claiming, the new dispatcher/handler/CsvUtil, the `Is_Batch__c` field and `Queued` status, and the live validation result. (Use the same GitHub-API-via-stored-credential approach used for PR #2, since `gh` is not installed.)

---

## Self-Review Notes (addressed)

- **Spec coverage:** routing (Task 6/7), 1,000-per-callout + threshold (Task 6 `BATCH_SIZE`, single-vs-batch branch), 1-batch-per-execution (Task 4), 25,000/run + self-chaining (Task 6 `MAX_PER_RUN`), multipart (Task 3), `Is_Batch__c` + `Queued` (Task 1), batch handler + CsvUtil (Tasks 2/5), reaper (Task 6), tests (Tasks 2–7), live validation (Task 9). All spec sections map to a task.
- **`MessageQueueProcessor` unchanged:** confirmed — heavy-aware *claiming* (Task 4) bounds callouts per execution; the processor already chains via `claimPending`.
- **Type/name consistency:** `MultipartUpload` (fields `fileFieldName`/`fileName`/`fileContent`/`formFields`), `buildBatchMessage(List<Id>, String)`, `GeocodeDispatcher.dispatch()` / `dispatchQueued` / `BATCH_SIZE` / `MAX_PER_RUN`, `CsvUtil.parseLine`, handler class `GeocodeBatchResponseHandler` — used identically across tasks and tests.
- **Coordinate order:** Census coordinates are `longitude,latitude` (x,y); handler maps `coord[0]→Longitude`, `coord[1]→Latitude` (Task 5), matching the existing single-address handler.
