package FlightsPack;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class KeyDepartArrive implements WritableComparable<KeyDepartArrive> {
    private Text Arr_id, Dep_id;

    public KeyDepartArrive(){
        this.Arr_id = new Text();
        this.Dep_id = new Text();
    }

    public KeyDepartArrive(String Arr, String Dep){
        Arr_id = new Text(Arr);
        Dep_id = new Text(Dep);
    }


}
