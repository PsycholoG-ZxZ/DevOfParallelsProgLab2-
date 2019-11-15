package FlightsPack;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class KeyDepartArrive implements WritableComparable<KeyDepartArrive> {
    private Text aeroport_id;
}
