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
}
