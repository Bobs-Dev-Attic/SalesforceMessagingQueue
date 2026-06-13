/**
 * EXAMPLE producer trigger for the Census geocoding use case.
 *
 * When a Geocode_Request__c is created in the Pending state, it enqueues a GET to
 * the U.S. Census "onelineaddress" geocoder. The scheduled processor makes the
 * callout ~60s later and CensusGeocodeResponseHandler writes the coordinates back
 * onto this record, which in turn fires the Geocode Post Processing flow.
 *
 * Census Geocoder is a public, key-less API. Because it is called by absolute URL
 * (not a Named Credential) the "Census Geocoder" Remote Site Setting must be active.
 * Docs: https://geocoding.geo.census.gov/geocoder/
 */
trigger GeocodeRequestTrigger on Geocode_Request__c (after insert) {

    final String BASE = 'https://geocoding.geo.census.gov/geocoder/locations/onelineaddress';

    List<Message_Queue__c> messages = new List<Message_Queue__c>();

    for (Geocode_Request__c req : Trigger.new) {
        if (req.Geocode_Status__c != 'Pending') {
            continue;
        }

        // Assemble a one-line address from whichever components are populated.
        List<String> parts = new List<String>();
        if (String.isNotBlank(req.Street__c)) { parts.add(req.Street__c); }
        if (String.isNotBlank(req.City__c))   { parts.add(req.City__c); }
        if (String.isNotBlank(req.State__c))  { parts.add(req.State__c); }
        if (String.isNotBlank(req.Zip__c))    { parts.add(req.Zip__c); }
        if (parts.isEmpty()) {
            continue;
        }

        String endpoint = BASE
            + '?address=' + EncodingUtil.urlEncode(String.join(parts, ', '), 'UTF-8')
            + '&benchmark=Public_AR_Current'
            + '&format=json';

        messages.add(
            MessageQueueService.buildMessage(
                'GET',
                endpoint,
                null,                            // GET has no body
                new Map<String, String>{ 'Accept' => 'application/json' },
                'CensusGeocodeResponseHandler',  // how to process the response
                req.Id                           // related record to write results back to
            )
        );
    }

    if (!messages.isEmpty()) {
        // enqueue() applies windowed deduplication before inserting.
        MessageQueueService.enqueue(messages);
    }
}
