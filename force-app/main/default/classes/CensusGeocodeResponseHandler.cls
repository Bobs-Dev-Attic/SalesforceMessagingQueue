/**
 * Response handler for the Census geocoding example.
 *
 * Parses the U.S. Census Geocoder JSON and writes the result back onto the
 * originating Geocode_Request__c (coordinates, matched address, raw response and a
 * status). Setting Geocode_Status__c to 'Matched' is what fires the record-triggered
 * "Geocode Post Processing" flow.
 *
 * Census response shape:
 * {
 *   "result": {
 *     "addressMatches": [
 *       { "matchedAddress": "...", "coordinates": { "x": -77.03, "y": 38.89 } }
 *     ]
 *   }
 * }
 * where x = longitude and y = latitude.
 *
 * Runs in the processor's callout transaction after callouts complete: DML here is
 * fine, further callouts are not. On a non-2xx response it leaves Status as
 * 'Processing' so the queue engine applies its retry/backoff policy.
 */
public with sharing class CensusGeocodeResponseHandler implements MessageQueueResponseHandler {

    public void handle(Message_Queue__c message, HttpResponse response) {
        if (String.isBlank(message.Related_Record_Id__c)) {
            return; // nothing to write the result back to
        }

        Geocode_Request__c req = new Geocode_Request__c(Id = message.Related_Record_Id__c);
        req.Geocoded_At__c = System.now();
        req.Geocode_Response__c = abbreviate(response.getBody(), 131072);

        Integer status = response.getStatusCode();
        if (status < 200 || status >= 300) {
            // Transport/server error: let the engine retry; record nothing terminal.
            return;
        }

        try {
            Map<String, Object> root = (Map<String, Object>) JSON.deserializeUntyped(response.getBody());
            Map<String, Object> result = (Map<String, Object>) root.get('result');
            List<Object> matches = result == null ? null : (List<Object>) result.get('addressMatches');

            if (matches != null && !matches.isEmpty()) {
                Map<String, Object> match = (Map<String, Object>) matches[0];
                req.Matched_Address__c = abbreviate(String.valueOf(match.get('matchedAddress')), 255);

                Map<String, Object> coords = (Map<String, Object>) match.get('coordinates');
                if (coords != null) {
                    req.Location__Latitude__s = toDecimal(coords.get('y'));   // y = latitude
                    req.Location__Longitude__s = toDecimal(coords.get('x'));  // x = longitude
                }
                req.Geocode_Status__c = 'Matched';
            } else {
                req.Geocode_Status__c = 'No_Match';
            }
        } catch (Exception e) {
            req.Geocode_Status__c = 'Error';
            req.Geocode_Response__c = abbreviate('Parse error: ' + e.getMessage(), 131072);
        }

        update req;
        // 2xx: leave message.Status__c as 'Processing' so the engine marks it Completed.
    }

    private Decimal toDecimal(Object value) {
        return value == null ? null : Decimal.valueOf(String.valueOf(value));
    }

    private String abbreviate(String value, Integer maxLen) {
        if (value == null) {
            return null;
        }
        return value.length() <= maxLen ? value : value.abbreviate(maxLen);
    }
}
