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
        List<Id> ids = new List<Id>{ '001000000000001AAA', '001000000000002AAA' };
        String csv = '001000000000001AAA,4550 Montgomery Ave,Bethesda,MD,20814';
        Message_Queue__c m = MessageQueueService.buildBatchMessage(ids, csv);

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
        List<Id> ids = new List<Id>{ '001000000000001AAA' };
        String csv = '001000000000001AAA,4550 Montgomery Ave,Bethesda,MD,20814';
        Message_Queue__c m = MessageQueueService.buildBatchMessage(ids, csv);

        MessageQueueService.MessagePayload p = MessageQueueService.parsePayload(m);
        System.assertNotEquals(null, p.multipart, 'multipart block should survive serialize/parse');
        System.assertEquals('addressFile', p.multipart.fileFieldName);
        System.assertEquals(csv, p.multipart.fileContent);
        System.assertEquals('Public_AR_Current', p.multipart.formFields.get('benchmark'));
    }
}
