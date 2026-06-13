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
}
