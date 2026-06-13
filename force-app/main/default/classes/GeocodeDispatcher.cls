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
 *
 * A companion Schedulable, GeocodeDispatcherScheduler, can be scheduled as a periodic
 * safety-net sweep so stragglers get dispatched and failed batches get reaped even when
 * no new requests are being inserted.
 */
public with sharing class GeocodeDispatcher implements Queueable {

    @TestVisible private static final Integer BATCH_SIZE = 1000;
    // Bounded by the 10,000 DML-rows-per-transaction limit: each dispatched request
    // is marked 'Queued' (1 DML row), plus batch-message inserts and the reaper's
    // own (bounded) DML must also fit. The dispatcher self-chains past this ceiling.
    @TestVisible private static final Integer MAX_PER_RUN = 9000;
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
            } else {
                // No usable address: mark Error so it isn't re-selected as Pending forever.
                toMark.add(new Geocode_Request__c(Id = r.Id, Geocode_Status__c = 'Error'));
            }
        } else {
            for (Integer i = 0; i < pending.size(); i += BATCH_SIZE) {
                List<Geocode_Request__c> chunk = new List<Geocode_Request__c>();
                Integer endIdx = Math.min(i + BATCH_SIZE, pending.size());
                for (Integer j = i; j < endIdx; j++) {
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
        }
        if (!toMark.isEmpty()) {
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
            ORDER BY CreatedDate ASC
            LIMIT 50
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
            LIMIT 500
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
