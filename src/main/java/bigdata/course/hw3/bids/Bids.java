package bigdata.course.hw3.bids;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * The Driver for the Bids app, supports handling of generic command-line options.
 */
public class Bids extends Configured implements Tool {

    private final static Logger LOGGER = Logger.getLogger(Bids.class);

    /**
     * Main method, invokes method run.
     *
     * @param args - command specific arguments.
     * @see #run(String[] args)
     */
    public static void main(String[] args) throws Exception {

        LOGGER.info("starting the app...");
        int result = ToolRunner.run(new Configuration(), new Bids(), args);
        LOGGER.info("...the execution of the app is finished! \n");

        System.exit(result);
    }

    /**
     * This method parses the args,
     * sets the configuration of the job and
     * submits the job to the cluster for execution.
     * <p>
     * The app support two additional properties:
     * [-D partitioner=custom] - to use custom partitioner
     * [-D num.reducers=N] - to set number of reducer tasks
     * If this properties aren't set, default partitioner and one reducer will be used
     * <p>
     * Also generic option -files "path/city.en.txt" must be set
     * to put look up table for city id to the distributed cache
     *
     * @param args - command specific arguments.
     * @return - result of the execution, 0 - if the job succeeded
     */
    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.print("Usage: Bids [-D options] <input path> <output path> \n");
            printProperties();
            return -1;
        }

        String hdfsInputPath = args[0];
        String hdfsOutputPath = args[1];

        Configuration conf = getConf();

        Class<? extends Partitioner> partitionerClass = getPartitioner(conf);
        int numReduceTasks = getNumReduceTasks(conf);

        LOGGER.info("partitioner: " + partitionerClass.getName());
        LOGGER.info("number of reducers: " + numReduceTasks + "\n");

        Job job = Job.getInstance(conf, "HW3 - Bids");
        job.setJarByClass(getClass());

        job.setMapperClass(BidsMapper.class);
        job.setCombinerClass(BidsCombiner.class);
        job.setReducerClass(BidsReducer.class);
        job.setPartitionerClass(partitionerClass);
        job.setNumReduceTasks(numReduceTasks);

        job.setMapOutputKeyClass(CompositeCity.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(hdfsInputPath));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputPath));

        return job.waitForCompletion(true) ? 0 : 1;

    }

    private Class<? extends Partitioner> getPartitioner(Configuration conf) {

        String partitioner = conf.get("partitioner");
        if (partitioner != null) {
            if (partitioner.equals("custom")) {
                return BidsPartitionerByOS.class;
            } else {
                printProperties();
                System.exit(-1);
            }
        }
        return HashPartitioner.class;
    }

    private int getNumReduceTasks(Configuration conf) {

        String numReducers = conf.get("num.reducers");
        if (numReducers != null) {
            int num = Integer.parseInt(numReducers);
            if (num > 0) {
                return num;
            } else {
                printProperties();
                System.exit(-1);
            }
        }
        return 1;
    }

    private void printProperties() {

        System.out.println("properties: \n" +
                "\t partitioner=custom \n " +
                "\t num.reducers=N (where N is positive number of reducers) \n" +
                "\t (if this properties aren't set, default partitioner and one reducer will be used) \n +" +
                "\t also generic option -files <path to the file>/city.en.txt must be specified");

    }
}
