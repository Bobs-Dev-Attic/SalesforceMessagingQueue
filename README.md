# SalesforceMessagingQueue

A durable, JSON-driven **outbound message queue** for Salesforce. Business
triggers drop messages onto a queue; a Scheduled Apex job drains the queue every
**60 seconds**, makes the external API callout each message describes, and routes
the response to a pluggable handler.

```
Trigger (e.g. Account)                 every 60s
   │ enqueue                  ┌──────────────────────────┐
   ▼                          │  MessageQueueScheduler    │  (Schedulable, CRON "0 * * * * ?")
┌─────────────────┐  claim    │  • claimPending()         │
│ Message_Queue__c │◀─────────│  • enqueue Queueable      │
│  (Pending/Retry) │          └────────────┬─────────────┘
└─────────────────┘                        │ System.enqueueJob
        ▲                                   ▼
        │ update status      ┌──────────────────────────────────────┐
        └────────────────────│  MessageQueueProcessor (Queueable,    │
                             │  Database.AllowsCallouts)              │
                             │  Phase 1: HTTP callouts                │
                             │  Phase 2: handler + status + DML       │
                             └────────────────────┬───────────────────┘
                                                  ▼
                                  MessageQueueResponseHandler
                                  (e.g. AccountSyncResponseHandler)
```

## Salesforce objects

A single custom object, **`Message_Queue__c`**, is the queue. Key fields:

| Field | Type | Purpose |
|-------|------|---------|
| `Name` (`MQ-{00000000}`) | Auto Number | Human-readable id |
| `Payload__c` | Long Text (JSON) | **The instruction set**: method, endpoint, headers, body, timeout, success codes, response handler |
| `Status__c` | Picklist | `Pending` → `Processing` → `Completed` / `Retry` / `Failed` / `Cancelled` |
| `HTTP_Method__c` / `Endpoint__c` | Picklist / Text | Reporting copies of the verb & target (payload is source of truth) |
| `Response_Body__c` / `Response_Status_Code__c` | Long Text / Number | Captured response of the latest attempt |
| `Attempts__c` / `Max_Attempts__c` | Number | Retry accounting (default max 3) |
| `Next_Attempt_At__c` | DateTime | Exponential backoff gate for `Retry` rows |
| `Scheduled_At__c` | DateTime | Earliest processing time (delayed/future delivery) |
| `Processed_At__c` | DateTime | When it reached `Completed` |
| `Error_Message__c` | Long Text | Diagnostic detail on failure |
| `Priority__c` | Number | Lower drains first (default 5) |
| `Response_Handler__c` | Text | Fallback handler class when the payload omits one |
| `Related_Record_Id__c` | Text(18) | Originating record id |
| `Correlation_Id__c` | Text (External Id, Unique) | Caller-supplied idempotency key |

A `Message_Queue_Admin` permission set grants full access to the object and engine.

> Want it tamper-proof / no storage cost? The same design works on a **Platform
> Event** instead of a custom object — but you lose retry state and queryable
> history, so a custom object is the better default for a retrying queue.

## Payload format

```jsonc
{
  "method": "POST",
  "endpoint": "callout:External_System/api/v1/accounts",   // Named Credential
  "headers": { "Content-Type": "application/json" },
  "body": { "salesforceId": "001...", "name": "Acme" },     // string sent as-is, else JSON-serialized
  "timeout": 20000,                                          // ms (optional)
  "successStatusCodes": [200, 201, 202],                     // optional; default 2xx
  "responseHandler": "AccountSyncResponseHandler"            // optional Apex class
}
```

## Apex components

| Class / Trigger | Role |
|-----------------|------|
| `MessageQueueScheduler` | `Schedulable`; CRON `0 * * * * ?` fires every minute, claims a batch, enqueues the processor. `start()` / `stop()` helpers. |
| `MessageQueueProcessor` | `Queueable, Database.AllowsCallouts`; two-phase (callouts, then handlers+DML), chains if backlog remains. |
| `MessageQueueService` | Build/enqueue, `claimPending` (row-locked), callout execution, response interpretation, retry/backoff. |
| `MessageQueueResponseHandler` | Interface for pluggable "how to process the response" logic. |
| `AccountSyncResponseHandler` | Example handler (treats 409 as success, captures remote id). |
| `MessageQueueTrigger` | Guards `Message_Queue__c`: defaults + payload validation. |
| `AccountToMessageQueueTrigger` | **Example producer** — enqueues a message when an Account is created/renamed. |
| `GeocodeRequestTrigger` / `CensusGeocodeResponseHandler` | **Census geocoding example** — enqueue a geocode call and write the result back to `Geocode_Request__c`. |

## Worked example: Census geocoding

A self-contained example geocodes an address via the public
[U.S. Census Geocoder](https://geocoding.geo.census.gov/geocoder/) and feeds the
result into a Flow:

1. Insert a **`Geocode_Request__c`** (Street/City/State/ZIP). It defaults to `Pending`.
2. **`GeocodeRequestTrigger`** enqueues a `GET` to
   `…/geocoder/locations/onelineaddress?address=…&benchmark=Public_AR_Current&format=json`,
   routed to `CensusGeocodeResponseHandler`.
3. ~60 s later the processor calls Census, and **`CensusGeocodeResponseHandler`**
   writes the coordinates (`Location__c`), `Matched_Address__c`, raw
   `Geocode_Response__c`, and sets `Geocode_Status__c = Matched` (or `No_Match` / `Error`).
4. That status change fires the **`Geocode Post Processing`** record-triggered flow,
   the hook for your downstream logic (it ships marking `Post_Processing_Complete__c`).

```
Geocode_Request__c (Pending)
  → GeocodeRequestTrigger → Message_Queue__c
  → [60s] MessageQueueProcessor → Census API
  → CensusGeocodeResponseHandler writes Location/Status back
  → "Geocode Post Processing" flow runs
```

Census is key-less and called by absolute URL, so the **`Census Geocoder` Remote
Site Setting** (included) must be active. To try it:

```apex
insert new Geocode_Request__c(
    Street__c = '1600 Pennsylvania Ave NW',
    City__c   = 'Washington', State__c = 'DC', Zip__c = '20500'
);
```

## Why 60 seconds (and not less)

Apex Scheduled jobs are driven by a CRON expression whose finest practical
granularity is one minute (`0 * * * * ?`). Sub-minute scheduling is not offered by
the platform scheduler, so 60 seconds is the floor. Under heavy backlog the
processor **chains** itself (`System.enqueueJob`) so work drains faster than one
batch per minute rather than waiting for the next tick.

## Deploy & enable

```bash
# Authorize an org, then:
sf project deploy start --source-dir force-app

# Register the every-60-second job:
sf apex run --file scripts/apex/setup.apex

# Run tests:
sf apex run test --class-names MessageQueueTest --result-format human
```

After deploying, create a **Named Credential** called `External_System` (or edit
`AccountToMessageQueueTrigger` / your payloads to point at your endpoint) so the
callouts have somewhere to go.
