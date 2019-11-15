package FlightsPack;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Iterator;

public class AirportsReducer extends Reducer<KeyDepartArrive, Text, Text, Text> {
    Iterator<Text> iter = value.iterator();
    Text ID = new Text (iter.next());
    double MidDelay = 0;
    double HighDelay = 0;
    double LowDelay = 999999999;
    double sum = 0;
    int i = 0;
}
