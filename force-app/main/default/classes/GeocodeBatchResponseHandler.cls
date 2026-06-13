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
