/**
 * Minimal RFC-4180-ish CSV line parser. Splits one line into fields, honoring
 * double-quoted fields (which may contain commas) and escaped quotes (""). Apex
 * has no built-in CSV parser; the Census batch geocoder returns quoted CSV whose
 * matched-address and coordinate fields contain commas.
 */
public with sharing class CsvUtil {

    public static List<String> parseLine(String line) {
        List<String> result = new List<String>();
        if (line == null) {
            return result;
        }
        Boolean inQuotes = false;
        String current = '';
        Integer len = line.length();
        for (Integer i = 0; i < len; i++) {
            String c = line.substring(i, i + 1);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < len && line.substring(i + 1, i + 2) == '"') {
                        current += '"';   // escaped quote
                        i++;
                    } else {
                        inQuotes = false; // closing quote
                    }
                } else {
                    current += c;
                }
            } else if (c == '"') {
                inQuotes = true;
            } else if (c == ',') {
                result.add(current);
                current = '';
            } else {
                current += c;
            }
        }
        result.add(current);
        return result;
    }
}
