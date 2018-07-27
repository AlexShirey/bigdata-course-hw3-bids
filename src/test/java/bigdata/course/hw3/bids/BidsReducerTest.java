package bigdata.course.hw3.bids;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class BidsReducerTest {

    private ReduceDriver<CompositeCity, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {

        BidsReducer reducer = new BidsReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void reduceTest() throws IOException {

        reduceDriver.withInput(new CompositeCity(234, "WINDOWS_XP"), Arrays.asList(new IntWritable(1), new IntWritable(1)));
        reduceDriver.withInput(new CompositeCity(226, "WINDOWS_XP"), Arrays.asList(new IntWritable(1), new IntWritable(5)));
        reduceDriver.withInput(new CompositeCity(220, "WINDOWS_XP"), Arrays.asList(new IntWritable(1), new IntWritable(3)));

        reduceDriver.withOutput(new Text("zhongshan"), new IntWritable(2));
        reduceDriver.withOutput(new Text("zhaoqing"), new IntWritable(6));
        reduceDriver.withOutput(new Text("zhuhai"), new IntWritable(4));

        reduceDriver.runTest();
    }

}