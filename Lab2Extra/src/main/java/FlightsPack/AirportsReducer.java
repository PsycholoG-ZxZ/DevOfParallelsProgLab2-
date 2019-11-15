package FlightsPack;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AirportsReducer extends Reducer<KeyDepartArrive, Text, Text, Text> {
    protected void reduce (KeyDepartArrive key, Iterable<Text> value, Context context) throws IOException,InterruptedException {
        Iterator<Text> iter = value.iterator();
        Text delay = new Text(iter.next());
        double dd = Double.parseDouble(delay.toString());
        double old_dd = dd;
        while (iter.hasNext()){
            String del_string =iter.next().toString();
            if (del_string != "") {
                double del = Double.parseDouble(del_string);
                if (dd < del) dd = del;
            }
        }

        String a = key.getArr_id();
        String b = key.getDep_id();
        String itg = " "+old_dd + " " + dd ;
        Text out = new Text(itg);
        context.write (delay, out );
    }
}
