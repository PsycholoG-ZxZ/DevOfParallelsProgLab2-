package FlightDelaysPack;

import FlightsPack.KeyDepartArrive;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AirPartitioner extends Partitioner <KeyDepartArrive, Text> {
    public int getPartition(KeyDepartArrive key, Text value, int numReduceTasks) {
        return (key.getArr_id().hashCode()& key.getDep_id().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}