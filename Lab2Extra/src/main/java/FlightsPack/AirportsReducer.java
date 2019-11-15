package FlightsPack;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AirportsReducer extends Reducer<KeyDepartArrive, Text, Text, Text> {
    protected void reduce (KeyDepartArrive key, Iterable<Text> value, Context context) throws IOException,InterruptedException {
        Iterator<Text> iter = value.iterator();
        Text delay = new Text(iter.next());
        int count = 0;
        double MaxDel = -999999;
        int all_count = 0;

        if (!delay.toString().equals("")) {
            double dd = Double.parseDouble(delay.toString());
            if (MaxDel < dd)
                MaxDel = dd;
            all_count++;
        }else {
            count++;
            all_count++;
        }
        while (iter.hasNext()){
            String string_delay = iter.next().toString();
            if (!delay.toString().equals("")) {
                double dd = Double.parseDouble(delay.toString());
                if (MaxDel < dd)
                    MaxDel = dd;
                all_count++;
            }else {
                count++;
                all_count++;
            }
        }

        String a = key.getArr_id();
        String b = key.getDep_id();
        String itg = a +" " + b;
        double per = all_count / count;
        Text out = new Text("Max: " + MaxDel + " Percent: " + per);
        context.write (new Text(itg), out);
    }
}
