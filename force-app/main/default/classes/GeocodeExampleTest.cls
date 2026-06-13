/**
 * Tests the Census geocoding example end-to-end: producer trigger enqueues a
 * message, the processor calls the (mocked) Census API, CensusGeocodeResponseHandler
 * writes the result back to Geocode_Request__c, and the post-processing flow fires.
 */
@IsTest
private class GeocodeExampleTest {

    private static final String MATCH_BODY =
        '{"result":{"input":{"address":{"address":"1600 Pennsylvania Ave NW, Washington, DC, 20500"}},' +
        '"addressMatches":[{"matchedAddress":"1600 PENNSYLVANIA AVE NW, WASHINGTON, DC, 20500",' +
        '"coordinates":{"x":-77.03654,"y":38.89766}}]}}';

    private static final String NO_MATCH_BODY = '{"result":{"addressMatches":[]}}';

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

    @IsTest
    static void triggerEnqueuesCensusCallout() {
        Geocode_Request__c req = insertRequest();

        List<Message_Queue__c> msgs = [
            SELECT HTTP_Method__c, Payload__c, Response_Handler__c
            FROM Message_Queue__c WHERE Related_Record_Id__c = :req.Id
        ];
        System.assertEquals(1, msgs.size(), 'One geocode message should be enqueued');
        System.assertEquals('GET', msgs[0].HTTP_Method__c);
        System.assertEquals('CensusGeocodeResponseHandler', msgs[0].Response_Handler__c);
        System.assert(msgs[0].Payload__c.contains('geocoding.geo.census.gov'),
            'Payload should target the Census geocoder');
        System.assert(msgs[0].Payload__c.contains('benchmark=Public_AR_Current'),
            'Payload should include the Census benchmark');
    }

    @IsTest
    static void matchWritesCoordinatesAndFiresFlow() {
        Geocode_Request__c req = insertRequest();

        Test.setMock(HttpCalloutMock.class, new MessageQueueMock(200, MATCH_BODY));
        Test.startTest();
        new MessageQueueScheduler().execute(null); // claim + enqueue processor
        Test.stopTest();                            // processor runs the callout

        req = [
            SELECT Geocode_Status__c, Matched_Address__c, Geocoded_At__c,
                   Post_Processing_Complete__c, Location__Latitude__s, Location__Longitude__s,
                   Geocode_Response__c
            FROM Geocode_Request__c WHERE Id = :req.Id
        ];
        System.assertEquals('Matched', req.Geocode_Status__c);
        System.assert(Math.abs(req.Location__Latitude__s - 38.89766) < 0.00001, 'Latitude (y) written');
        System.assert(Math.abs(req.Location__Longitude__s - (-77.03654)) < 0.00001, 'Longitude (x) written');
        System.assert(req.Matched_Address__c.startsWith('1600 PENNSYLVANIA'), req.Matched_Address__c);
        System.assertNotEquals(null, req.Geocoded_At__c);
        System.assertNotEquals(null, req.Geocode_Response__c, 'Raw response captured');
        System.assertEquals(true, req.Post_Processing_Complete__c,
            'Post-processing flow should have run on the Matched record');

        System.assertEquals('Completed',
            [SELECT Status__c FROM Message_Queue__c WHERE Related_Record_Id__c = :req.Id].Status__c);
    }

    @IsTest
    static void noMatchSetsStatusAndDoesNotFireFlow() {
        Geocode_Request__c req = insertRequest();

        Test.setMock(HttpCalloutMock.class, new MessageQueueMock(200, NO_MATCH_BODY));
        Test.startTest();
        new MessageQueueScheduler().execute(null);
        Test.stopTest();

        req = [
            SELECT Geocode_Status__c, Post_Processing_Complete__c, Location__Latitude__s
            FROM Geocode_Request__c WHERE Id = :req.Id
        ];
        System.assertEquals('No_Match', req.Geocode_Status__c);
        System.assertEquals(null, req.Location__Latitude__s, 'No coordinates on a no-match');
        System.assertEquals(false, req.Post_Processing_Complete__c,
            'Flow only runs for Matched records');
    }
}
