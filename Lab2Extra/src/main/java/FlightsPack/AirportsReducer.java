package FlightsPack;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AirportsReducer extends Reducer<KeyDepartArrive, Text, Text, Text> {
    protected void reduce (KeyDepartArrive key, Iterable<Text> value, Context context) throws IOException,InterruptedException {
        Iterator<Text> iter = value.iterator();
        Text delay = new Text(iter.next());
        int count = 0; //canceled
        int countD = 0;
        double MaxDel = 0;
        int all_count = 0;

        if (!delay.toString().equals("")) {
            double dd = Double.parseDouble(delay.toString());
            if (MaxDel < dd)
                MaxDel = dd;
            all_count++;
            countD = 0;
        }else {
            count++;
            all_count++;
        }

        while (iter.hasNext()){
            String string_delay = iter.next().toString();
            if (!string_delay.toString().equals("")) {
                double dd = Double.parseDouble(string_delay);
                if (MaxDel < dd)
                    MaxDel = dd;
                all_count++;
                countD = 0;
            }else {
                count++;
                all_count++;
            }
        }
/*
        String itgg = delay.toString();
        while (iter.hasNext()){
            String string_delay = iter.next().toString();
            itgg = itgg +" " +string_delay;
        }
*/
        String a = key.getArr_id();
        String b = key.getDep_id();
        String itg = a +" " + b;
        double per;
        if (count != 0) {
            per = all_count / count;
        }else{
            per =all_count;
        }
        Text out = new Text("Max: " + MaxDel + " Percent: " + per);
        context.write (new Text(itg), out);
        //context.write (new Text(itg), new Text(itgg));
    }
}
