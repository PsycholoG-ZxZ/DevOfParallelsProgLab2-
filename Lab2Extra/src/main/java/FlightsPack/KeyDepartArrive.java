package FlightsPack;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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

    @Override
    public void readFields(DataInput inpt) throws IOException {
        aeroport_id.readFields(inpt);
        base.readFields(inpt);
    }

    public static int compare (int a, int b){
        return Integer.compare(a,b);
    }

    @Override
    public void write (DataOutput out) throws IOException{
        aeroport_id.write(out);
        base.write(out);
    }
}
