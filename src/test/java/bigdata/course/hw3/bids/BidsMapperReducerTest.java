package bigdata.course.hw3.bids;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;


public class BidsMapperReducerTest {

    private MapReduceDriver<LongWritable, Text, CompositeCity, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {

        BidsMapper mapper = new BidsMapper();
        BidsReducer reducer = new BidsReducer();
        BidsCombiner combiner = new BidsCombiner();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer, combiner);
    }

    @Test
    public void mapReduceTest() {

        mapReduceDriver.withInput(new LongWritable(1), new Text("2e72d1bd7185fb76d69c852c57436d37\t20131019025500549\t1\tCAD06D3WCtf\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t113.117.187.*\t216\t234\t2\t33235ca84c5fee9254e6512a41b3ad5e\t8bbb5a81cc3d680dd0c27cf4886ddeae\tnull\t3061584349\t728\t90\tOtherView\tNa\t5\t7330\t277\t48\tnull\t2259\t10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063"));
        mapReduceDriver.withInput(new LongWritable(2), new Text("4c5d659976877301764ec693e76f0aad\t20131019083602285\t1\tD5RD4UERcUt\tMozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)\t121.10.252.*\t216\t226\t1\t77f076dcd9afc63aeb0b8ca7fae70b06\te1468015c9b3c90dcda04d0211a2a82\tnull\tmm_14200188_3431502_11322724\t250\t250\tNa\tNa\t0\t7321\t294\t71\tnull\t2259\t13800,10075,10006,13866,10111,10063,10116"));
        mapReduceDriver.withInput(new LongWritable(3), new Text("161e99b52eae374254b90412ae8422cf\t20131019095401643\t1\tDAJ9r1A4uGQ\tMozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.472.33 Safari/534.3 SE 2.X MetaSr 1.0\t112.90.231.*\t216\t220\t2\t7ecfc8f746523ea852ee3ee2a708eae9\te62c738e4e524ef26ed57f8bf7309e3f\tnull\t3891636629\t200\t200\tOtherView\tNa\t5\t7319\t277\t48\tnull\t2259\tnull"));

        mapReduceDriver.withOutput(new Text("zhongshan"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("zhaoqing"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("zhuhai"), new IntWritable(1));
    }

}
