package FlightsPack;

import org.apache.hadoop.io.Text;

public class CSVParser {
    private static String SEC_TABLE="\"ARR_DELAY\"";

    public static String[] parseSmall(Text str) {
        String[] data = str.toString().split(",");
        return data;

    }
}
