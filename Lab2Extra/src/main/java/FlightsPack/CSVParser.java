package FlightsPack;

import org.apache.hadoop.io.Text;




public class CSVParser {

    private static String FRST_TABLE="Code,Description";
    private static String SEC_TABLE="\"ARR_DELAY\"";

    public static String[] parseIdDescr(Text str) {
        String[] data = str.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        return data;
    }
    public static String[] parseSmall(Text str) {
        String[] data = str.toString().split(",");
        return data;

    }


    public static boolean CheckStringFrstCsv(String id) {
        return id.equals(FRST_TABLE);
    }
    public static boolean CheckStringSecCsv(String id) {
        return ((id.equals(SEC_TABLE)));
    }

    public static String RemoveSlash(String id) {
        return id.replace("\"", "");
    }



}
