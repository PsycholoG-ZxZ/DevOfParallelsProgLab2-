package FlightsPack;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, KeyDepartArrive, Text>  {

    private static int DELAY = 17, ID_AIRPORT = 14, SEC_TABLE_ID = 1;

    @Override

    protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
        String data[] =CSVParser.parseSmall(value);
        if (CSVParser.CheckStringSecCsv(data[DELAY])) {return; }
        String id = data[ID_AIRPORT];
        KeyDepartArrive aKey = new KeyDepartArrive(CSVParser.RemoveSlash(id), );
        Text title = new Text(data[DELAY]);
        context.write(aKey,title);
    }
}
