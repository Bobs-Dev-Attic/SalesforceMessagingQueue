/**
 * Response handler for the Census BATCH geocoder. Parses the CSV response and
 * writes results back onto each originating Geocode_Request__c. The CSV's first
 * column is the Geocode_Request__c Id we uploaded, which Census echoes back.
 *
 * Census batch CSV columns (no header), quoted:
 *   ID, Input Address, Match Indicator, Match Type, Matched Address,
 *   Coordinates ("longitude,latitude"), TIGER Line ID, Side
 *
 * A 'Tie' indicator (address matched multiple candidates ambiguously) carries no
 * coordinates from the batch API, so it is recorded as 'No_Match'.
 *
 * On a non-2xx response it leaves message.Status__c as 'Processing' so the queue
 * engine applies its retry/backoff policy to the whole batch.
 *
 * Uploaded rows absent from a 2xx response (Census silently omitted them) are
 * reconciled to 'No_Match' so no uploaded request is left stranded in 'Queued'.
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

        for (String rawLine : body.split('\n')) {
            String line = rawLine == null ? '' : rawLine.trim(); // tolerate CRLF line endings
            if (String.isBlank(line)) {
                continue;
            }
            List<String> cols = CsvUtil.parseLine(line);
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

        // Reconcile: any uploaded request whose row was absent from the 2xx response
        // must still leave 'Queued'. The uploaded CSV (with all Ids in column 0) is in
        // the message payload. Mark missing ones No_Match so they aren't stranded.
        for (Id uploadedId : uploadedRequestIds(message)) {
            if (!updates.containsKey(uploadedId)) {
                updates.put(uploadedId, new Geocode_Request__c(
                    Id = uploadedId,
                    Geocode_Status__c = 'No_Match',
                    Geocoded_At__c = System.now()));
            }
        }

        if (!updates.isEmpty()) {
            update updates.values();
        }
        // 2xx: leave message.Status__c as 'Processing' so the engine marks it Completed.
    }

    /** Ids of every request we uploaded, read from the batch CSV in the message payload. */
    private Set<Id> uploadedRequestIds(Message_Queue__c message) {
        Set<Id> ids = new Set<Id>();
        if (message == null || String.isBlank(message.Payload__c)) {
            return ids;
        }
        try {
            Map<String, Object> raw = (Map<String, Object>) JSON.deserializeUntyped(message.Payload__c);
            Object mp = raw.get('multipart');
            if (mp instanceof Map<String, Object>) {
                String csv = (String) ((Map<String, Object>) mp).get('fileContent');
                if (csv != null) {
                    for (String row : csv.split('\n')) {
                        if (String.isBlank(row)) {
                            continue;
                        }
                        List<String> cols = CsvUtil.parseLine(row.trim());
                        if (!cols.isEmpty()) {
                            try {
                                ids.add((Id) cols[0]);
                            } catch (Exception ignore) {
                                // not an Id row
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            // unparseable payload -> nothing to reconcile
        }
        return ids;
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
