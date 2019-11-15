package FlightsPack;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AirportsReducer extends Reducer<KeyDepartArrive, Text, Text, Text> {
    protected void reduce (KeyDepartArrive key, Iterable<Text> value, Context context) throws IOException,InterruptedException {
        Iterator<Text> iter = value.iterator();
        Text delay = new Text(iter.next());

        int i = 0;

        //while (iter.hasNext()){
         //   i++;
        //}

        String a = key.getArr_id();
        String b = key.getDep_id();
        String itg = " " + i;
        Text out = new Text(itg);
        context.write (delay, out );
    }
}
