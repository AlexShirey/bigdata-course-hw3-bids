package bigdata.course.hw3.bids;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * The combiner class.
 * The same as reducer except writing to the context cityId rather than its name.
 */
public class BidsCombiner extends Reducer<CompositeCity, IntWritable, CompositeCity, IntWritable> {

    private IntWritable bidsCount = new IntWritable();

    @Override
    protected void reduce(CompositeCity key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        bidsCount.set(sum);
        context.write(key, bidsCount);
    }
}
