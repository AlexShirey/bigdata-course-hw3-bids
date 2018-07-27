package bigdata.course.hw3.bids;

import bigdata.course.hw3.bids.util.RecordParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class for the Bids app, writes to the context
 * custom WritableComparable CompositeCity as key with cityId and operation system type and
 * sets the IntWritable value = 1 for counting cities id
 */
public class BidsMapper extends Mapper<LongWritable, Text, CompositeCity, IntWritable> {

    private final static String USER_OS_GROUP = "User's OS";
    private CompositeCity compositeCity = new CompositeCity();
    private IntWritable one = new IntWritable(1);
    private RecordParser parser = new RecordParser();

    enum MapperCounter {
        INVALID_RECORD, BIDS_PRICE_GT_250, BIDS_PRICE_LT_251
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (!parser.parseRecord(value.toString())) {
            context.getCounter(BidsMapper.MapperCounter.INVALID_RECORD).increment(1);
            return;
        }

        int cityId = parser.getCityId();
        String os = parser.getOperationSystem();
        int bidPrice = parser.getBidPrice();

        compositeCity.setCityId(cityId);
        compositeCity.setOperationSystem(os);

        //dynamic counter, counts users per OS
        context.getCounter(USER_OS_GROUP, os).increment(1);

        //write to the context if bid price is more then 250
        if (bidPrice > 250) {
            context.write(compositeCity, one);
            context.getCounter(MapperCounter.BIDS_PRICE_GT_250).increment(1);
        } else {
            context.getCounter(MapperCounter.BIDS_PRICE_LT_251).increment(1);
        }
    }
}
