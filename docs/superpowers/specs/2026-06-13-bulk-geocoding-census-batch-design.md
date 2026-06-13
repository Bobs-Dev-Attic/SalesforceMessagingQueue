# Bulk Geocoding via the Census Batch Service — Design

**Date:** 2026-06-13
**Status:** Approved (pending spec review)
**Branch:** `feature/bulk-geocoding`

## Problem

The geocoding example currently makes **one HTTP callout per address**: a
`Geocode_Request__c` is inserted, `GeocodeRequestTrigger` enqueues a single
`Message_Queue__c` with a `GET` to the Census *onelineaddress* endpoint, and
`CensusGeocodeResponseHandler` writes the coordinates back.

This does not scale. Geocoding thousands of addresses would create thousands of
messages and thousands of callouts, quickly exhausting per-transaction callout
limits and taking a very long time.

The U.S. Census Geocoder offers a **batch** service that accepts up to 10,000
addresses in a single request. This design adds support for that service and
adjusts the Apex engine to handle large volumes within Salesforce governor
limits, with sensible, explicit caps.

## Goals

- Geocode large numbers of addresses efficiently using the Census batch service.
- Keep everything flowing through the existing durable `Message_Queue__c` engine
  (retry/backoff, scheduling, observability).
- Auto-route by volume: individual callouts for a single request, batch upload
  for many.
- Set reasonable, explicit limits that respect Salesforce governor limits.

## Non-goals

- No new UI. Requests are created by inserting `Geocode_Request__c` records
  (as today).
- No change to the generic account-sync example or the core queue semantics
  beyond what batch support requires.
- Not implementing the Census *geographies* batch endpoint (locations only).

## Key decisions (confirmed)

| Decision | Value |
|----------|-------|
| Integration model | Auto-route by volume, through the queue |
| Addresses per batch callout | **1,000** |
| Routing threshold | **2+** pending → batch; exactly 1 → single GET |
| Batch callouts per Queueable execution | **1, then chain** |
| Addresses dispatched per run | **9,000** (9 batches); remainder self-chained to a fresh job (bounded by the 10,000 DML-rows-per-transaction limit) |

## Census batch service reference

- **Endpoint:** `POST https://geocoding.geo.census.gov/geocoder/locations/addressbatch`
- **Content type:** `multipart/form-data`
- **Form fields:**
  - `addressFile` — a CSV file part. No header row. Columns:
    `Unique ID, Street, City, State, ZIP`. Up to 10,000 rows.
  - `benchmark` — e.g. `Public_AR_Current`.
- **Response:** CSV (no JSON option). One row per input, columns:
  `ID, Input Address, Match Indicator, Match Type, Matched Address, Coordinates (lon,lat), TIGER Line ID, Side`.
  Fields are comma-separated and quoted; `Matched Address` contains commas.
- Public, key-less API. Called by absolute URL, so the existing
  **Census Geocoder** Remote Site Setting covers it.

## Architecture

### Component overview

```
Insert Geocode_Request__c (Pending)
        │
        ▼
GeocodeRequestTrigger (after insert)
        │  enqueues ONE GeocodeDispatcher Queueable (in-transaction dedup)
        ▼
GeocodeDispatcher (Queueable)
        │  queries all Pending requests, routes by volume, marks them 'Queued'
        ├── 1 request  → MessageQueueService.buildMessage(...)  [GET, single]   → CensusGeocodeResponseHandler
        └── 2+ requests → chunk ≤1,000 → MessageQueueService.buildBatchMessage(...) [POST multipart]
                                                                                  → GeocodeBatchResponseHandler
        ▼
MessageQueueScheduler (hourly) / chained MessageQueueProcessor
        │  claims messages; processes at most ONE batch (heavy) callout per execution, then chains
        ▼
MessageQueueService.executeCallout
        │  builds multipart/form-data body when payload.multipart is present
        ▼
Census batch endpoint → CSV response
        ▼
GeocodeBatchResponseHandler.handle
        │  parses CSV, maps Unique ID → Geocode_Request__c, bulk-updates
        ▼
Geocode_Request__c set to Matched / No_Match / Error  →  Geocode Post Processing flow fires on Matched
```

### New / changed components

1. **`GeocodeRequestTrigger`** (changed)
   Stops creating messages inline. Instead enqueues a single `GeocodeDispatcher`
   per transaction (guarded by a static flag so a multi-chunk bulk insert
   enqueues only one). Keeps inserts flowing automatically with low latency.

2. **`GeocodeDispatcher`** (new, `Queueable`)
   - Queries `Geocode_Request__c WHERE Geocode_Status__c = 'Pending'`
     (ordered, `LIMIT` the per-run ceiling = 9,000).
   - If exactly 1 → build a single `GET` message (existing path/handler).
   - If 2+ → split into chunks of ≤1,000; for each chunk, build the CSV
     (`Id,Street,City,State,Zip` per row) and a batch message.
   - Sets the dispatched requests' `Geocode_Status__c = 'Queued'` in the same
     transaction as message creation (so a failure rolls back both).
   - Also exposed as a `static dispatch()` entry point callable from the
     scheduler as a straggler safety net.

3. **`MessageQueueService`** (changed)
   - `MessagePayload` gains an optional `multipart` block:
     ```
     class MultipartUpload {
       String fileFieldName;          // "addressFile"
       String fileName;               // "addresses.csv"
       String fileContent;            // the CSV text
       Map<String,String> formFields; // { "benchmark": "Public_AR_Current" }
     }
     ```
   - `executeCallout`: when `payload.multipart` is set, build a boundary-safe
     `multipart/form-data` body (the standard base64 alignment technique so the
     binary boundary survives `Blob`/`String` conversions) and set the
     `Content-Type: multipart/form-data; boundary=...` header. Otherwise behaves
     exactly as today.
   - New `buildBatchMessage(List<Id> requestIds, String csv)` helper constructs
     the batch `Message_Queue__c` (`Is_Batch__c = true`, multipart payload,
     `Response_Handler__c = 'GeocodeBatchResponseHandler'`).
   - `claimPending` / scheduler claim logic becomes "heavy-aware": at most **one**
     `Is_Batch__c = true` message is handed to a single processor execution
     (see Limits). Single (non-batch) messages continue to be claimed in bulk.

4. **`Message_Queue__c.Is_Batch__c`** (new field, Checkbox, default false)
   Lets the engine and claim logic recognize heavy batch messages without
   parsing the payload. Also useful for monitoring.

5. **`Geocode_Status__c`** (changed picklist) — add value **`Queued`**
   Marks a request that has been dispatched onto a message but not yet resolved,
   preventing re-dispatch by a later dispatcher run.

6. **`GeocodeBatchResponseHandler`** (new, implements `MessageQueueResponseHandler`)
   - On 2xx: parse the CSV response with a small quoted-field parser; for each
     row map the Unique ID to its `Geocode_Request__c`; set `Matched` +
     `Location` (lat/lon) + `Matched_Address__c`, or `No_Match`; bulk `update`.
   - On non-2xx: leave `message.Status__c = 'Processing'` so the engine retries.
   - Parse failure for a row → that request → `Error`.

7. **`CsvUtil`** (new, small helper)
   `parseLine(String) : List<String>` handling quoted fields and embedded commas;
   used by the batch handler. Kept separate so it is unit-testable in isolation.

8. **Batch failure reaper**
   When a batch message reaches `Failed` (retries exhausted), reset its
   associated requests `Queued → Error` so they are not stuck invisibly. Hook:
   `MessageQueueService.markFailureOrRetry` already centralizes the terminal
   `Failed` transition — the reaper runs from there (or from a dedicated check in
   the processor after the terminal update) for batch messages.

## Data flow detail: the unique-ID round trip

The CSV we upload uses the Salesforce record Id as the unique ID column. The
Census response echoes that ID in column 1, so the handler maps results straight
back to `Geocode_Request__c` records without needing to re-read the payload.

For the reaper (a permanently `Failed` batch that may have no usable response),
the set of dispatched request Ids is recovered by parsing column 1 of the CSV
stored in the message's `multipart.fileContent` (i.e. the payload we uploaded).
This avoids needing a separate child-link field from request to batch message.

## Limits and governor-limit rationale

- **1,000 addresses / callout.** Keeps each Census file small (~tens to low
  hundreds of KB) — well under the async heap (12 MB) and request-size (12 MB)
  limits, and usually returns within the 120 s per-callout timeout.
- **1 batch callout / Queueable execution, then chain.** Salesforce limits the
  **cumulative** callout time across a single transaction to **120 seconds**.
  Several slow batch calls in one transaction would exceed it, so each batch
  callout runs in its own chained transaction. Non-batch (single) messages remain
  cheap and are still processed in bulk per execution.
- **9,000 addresses / dispatch run.** Salesforce caps DML at **10,000 rows per
  transaction**. Each dispatched request is marked `Queued` (one DML row), so the
  per-run ceiling must stay under 10,000 with headroom for the batch-message
  inserts and the reaper's own (bounded) DML. When a run hits the ceiling the
  dispatcher self-chains a fresh Queueable to continue, so total throughput is
  unbounded across chained transactions. A periodic scheduled sweep
  (`GeocodeDispatcher` is also `Schedulable`) provides a safety-net that picks up
  any stragglers and runs the reaper even when no new requests are being inserted.

## Error handling

| Failure | Behavior |
|---------|----------|
| Multipart build error | Message → retry/backoff via engine; requests stay `Queued` |
| Non-2xx response | Handler leaves `Processing`; engine retries with backoff |
| Per-row parse error | That request → `Error`; others in the batch proceed |
| Batch message exhausts retries (`Failed`) | Reaper resets its requests `Queued → Error` |
| Census `No_Match` / `Tie` for a row | Request → `No_Match` (Tie treated as No_Match) |

Requests never leave `Queued` except to a terminal state, so retries never create
duplicate work.

## Testing strategy

**Unit (Apex tests, `HttpCalloutMock`):**
- Dispatcher routing: 1 pending → single GET message; 2 pending → one batch
  message; 1,500 pending → two batch messages (1,000 + 500); 9,000 pending →
  exactly 9 batches dispatched in one run, with a fresh job self-chained for any
  remainder.
- Multipart body construction: correct boundary, field parts, file part with CSV.
- `CsvUtil.parseLine`: plain row, quoted field with embedded comma, empty fields.
- `GeocodeBatchResponseHandler`: sample Census CSV with Match / No_Match / Tie /
  malformed rows → correct per-request status, lat/lon, matched address.
- Heavy-aware claim: a mix of batch + single messages hands at most one batch to
  a single processor execution.
- Reaper: a `Failed` batch message flips its `Queued` requests to `Error`.

**Live validation (manual, like the single-address test):**
- Insert a small set (e.g. 3–5) of real addresses, let the dispatcher batch them,
  drive a tick, and confirm the real Census batch endpoint returns matches and
  coordinates are written back. Requires the `Geocode_Request_Access` and
  `Message_Queue_Admin` permission sets (already in the repo).

## Affected files (anticipated)

**New:**
- `classes/GeocodeDispatcher.cls`
- `classes/GeocodeBatchResponseHandler.cls`
- `classes/CsvUtil.cls`
- `objects/Message_Queue__c/fields/Is_Batch__c.field-meta.xml`
- tests: `classes/GeocodeBatchTest.cls` (and additions to existing geocode test)

**Changed:**
- `classes/MessageQueueService.cls` (multipart, buildBatchMessage, heavy-aware claim, reaper hook)
- `classes/MessageQueueProcessor.cls` (heavy-aware execution / chaining)
- `classes/MessageQueueScheduler.cls` (optional straggler dispatch call)
- `triggers/GeocodeRequestTrigger.trigger` (enqueue dispatcher instead of inline messages)
- `objects/Geocode_Request__c/fields/Geocode_Status__c.field-meta.xml` (add `Queued`)
- `permissionsets/Message_Queue_Admin.permissionset-meta.xml` (FLS for `Is_Batch__c`)

## Open risks

- **Apex multipart correctness.** Building multipart/form-data by hand is the
  riskiest part; the live validation against the real endpoint is the gate that
  proves it works.
- **Census response-time variability.** Under load the service can be slow; the
  1,000-row cap plus per-transaction isolation plus retry/backoff are the
  mitigations.
