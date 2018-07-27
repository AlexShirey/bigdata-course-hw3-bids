package bigdata.course.hw3.bids;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * The custom partitioner that partitions keys by operation system rather than by city id.
 */
public class BidsPartitionerByOS extends Partitioner<CompositeCity, IntWritable> {

    @Override
    public int getPartition(CompositeCity compositeCity, IntWritable intWritable, int numPartitions) {

        return Math.abs(compositeCity.getOperationSystem().hashCode()) % numPartitions;
    }
}
