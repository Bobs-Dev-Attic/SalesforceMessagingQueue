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

    @IsTest
    static void csvParserKeepsTrailingEmptyField() {
        List<String> cols = CsvUtil.parseLine('a,b,');
        System.assertEquals(3, cols.size(), 'Trailing comma yields a trailing empty field');
        System.assertEquals('', cols[2]);
    }

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

    @IsTest
    static void multipartBodyIsWellFormed() {
        MessageQueueService.MultipartUpload mu = new MessageQueueService.MultipartUpload();
        mu.fileFieldName = 'addressFile';
        mu.fileName = 'addresses.csv';
        mu.fileContent = 'id1,4550 Montgomery Ave,Bethesda,MD,20814';
        mu.formFields = new Map<String, String>{ 'benchmark' => 'Public_AR_Current' };

        String boundary = '----TestBoundary';
        String body = MessageQueueService.buildMultipartBody(mu, boundary);

        // Form-field part comes first
        System.assert(body.contains('--' + boundary + '\r\nContent-Disposition: form-data; name="benchmark"\r\n\r\nPublic_AR_Current\r\n'),
            'benchmark form field part malformed: ' + body);
        // File part with filename + content type
        System.assert(body.contains('Content-Disposition: form-data; name="addressFile"; filename="addresses.csv"\r\n'),
            'file part disposition malformed: ' + body);
        System.assert(body.contains('Content-Type: text/csv\r\n\r\n'),
            'file part content-type/blank-line malformed: ' + body);
        System.assert(body.contains('id1,4550 Montgomery Ave,Bethesda,MD,20814\r\n'),
            'file content missing: ' + body);
        // Closing delimiter
        System.assert(body.endsWith('--' + boundary + '--\r\n'),
            'closing boundary malformed: ' + body);
    }

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

    @IsTest
    static void batchHandlerTreatsTieAsNoMatch() {
        Geocode_Request__c r = new Geocode_Request__c(
            Street__c = 'Ambiguous', City__c = 'Y', State__c = 'YY', Zip__c = '22222',
            Geocode_Status__c = 'Queued');
        insert r;
        // Census returns a 3-column row with "Tie" and no coordinates for ambiguous matches.
        String csv = '"' + r.Id + '","Ambiguous, Y, YY, 22222","Tie"';
        HttpResponse resp = new HttpResponse();
        resp.setStatusCode(200);
        resp.setBody(csv);

        new GeocodeBatchResponseHandler().handle(MessageQueueService.buildBatchMessage('x'), resp);

        Geocode_Request__c out = [
            SELECT Geocode_Status__c, Location__Latitude__s
            FROM Geocode_Request__c WHERE Id = :r.Id
        ];
        System.assertEquals('No_Match', out.Geocode_Status__c, 'Tie has no single coordinate; treated as No_Match');
        System.assertEquals(null, out.Location__Latitude__s);
    }

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
        // Next claim (same transaction) returns exactly one batch (heavy work one-at-a-time).
        List<Message_Queue__c> second = MessageQueueService.claimPending(100);
        Test.stopTest();

        // Singles are claimed first, in bulk; batch messages are not mixed in.
        System.assertEquals(1, first.size(), 'Single message claimed first, batches deferred');
        System.assertEquals(single.Id, first[0].Id);

        System.assertEquals(1, second.size(), 'At most one batch message per claim');
        System.assertEquals(true,
            [SELECT Is_Batch__c FROM Message_Queue__c WHERE Id = :second[0].Id].Is_Batch__c);
    }

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
    static void triggerEnqueuesDispatcherAndDispatches() {
        // No suppression: the after-insert trigger enqueues a real GeocodeDispatcher
        // Queueable, which runs at Test.stopTest() and produces the routed message.
        Test.startTest();
        insert makeRequests(1);
        Test.stopTest();

        System.assertEquals(1, [SELECT COUNT() FROM Message_Queue__c],
            'Trigger-enqueued dispatcher should produce one message');
        System.assertEquals(1, [SELECT COUNT() FROM Geocode_Request__c WHERE Geocode_Status__c = 'Queued'],
            'Dispatched request flipped to Queued');
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
}
