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

    public void readFields(DataInput inpt) throws IOException {
        Arr_id.readFields(inpt);
        Dep_id.readFields(inpt);
    }

    public String getArr_id(){
        return this.Arr_id;
    }

    public String getDep_id(){
        return this.Dep_id;
    }


    public void write (DataOutput out) throws IOException{
        Arr_id.write(out);
        Dep_id.write(out);
    }

    public int compareTo(KeyDepartArrive keyNeitr) {
        int act = Arr_id.compareTo(keyNeitr.Arr_id);
        if (act != 0)
            return act;
        return Dep_id.compareTo(keyNeitr.Dep_id);

    }

}
