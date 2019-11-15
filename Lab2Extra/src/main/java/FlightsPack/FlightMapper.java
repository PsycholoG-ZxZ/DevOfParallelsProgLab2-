package FlightsPack;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, KeyDepartArrive, Text>  {

    private static int DELAY = 17, ID_AIRPORT_DEST = 14, ID_AIRPORT_ARR = 11,  SEC_TABLE_ID = 1;

    @Override

    protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
        String data[] =CSVParser.parseSmall(value);
        if (CSVParser.CheckStringSecCsv(data[DELAY])) {
            return;
        }
        String Arr_id = data[ID_AIRPORT_ARR];
        String Dest_id = data[ID_AIRPORT_DEST];
        KeyDepartArrive aKey = new KeyDepartArrive(CSVParser.RemoveSlash(Arr_id), CSVParser.RemoveSlash(Dest_id) );
        Text title = new Text(data[DELAY]);
        context.write(aKey,title);
    }
}
