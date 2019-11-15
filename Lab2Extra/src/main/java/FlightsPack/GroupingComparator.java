package FlightsPack;

import FlightsPack.KeyDepartArrive;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator{
    protected GroupingComparator() {
        super(KeyDepartArrive.class, true);
    }

    public int compare (WritableComparable ComparablePairFrst, WritableComparable ComparablePairSec){
        KeyDepartArrive CP_Frst = (KeyDepartArrive)ComparablePairFrst;
        KeyDepartArrive CP_Sec = (KeyDepartArrive)ComparablePairSec;
        return CP_Frst.getArr_id().compareTo( CP_Sec.getArr_id());
    }
}
