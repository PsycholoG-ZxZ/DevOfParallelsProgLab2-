package FlightsPack;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AirportsReducer extends Reducer<KeyDepartArrive, Text, Text, Text> {
    protected void reduce (KeyDepartArrive key, Iterable<Text> value, Context context) throws IOException,InterruptedException {
        Iterator<Text> iter = value.iterator();
        Text check = new Text(iter.next());

        double MidDelay = 0;
        double HighDelay = 0;
        double LowDelay = 999999999;
        double sum = 0;
        int i = 0;
        String a = key.getArr_id();
        String b = key.getDep_id();
        String itg = a + " " + b;
        Text out = new Text(itg);
        context.write (check, out );
    }
}
