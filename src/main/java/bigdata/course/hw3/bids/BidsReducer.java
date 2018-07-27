package bigdata.course.hw3.bids;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

/**
 * The Reducer class for Bids app.
 * Counts the amount of values (bids) per key (grouped by city id),
 * and writes to the context city name (using mapping from lookup table) and its amount of bids.
 */
public class BidsReducer extends Reducer<CompositeCity, IntWritable, Text, IntWritable> {

    private Text cityName = new Text();
    private IntWritable bidsCount = new IntWritable();
    private HashMap<Integer, String> cityMetaData;

    /**
     * Writes to the cityMetaData HashMap mapping for city id from
     * the look up table that is located in the distributed cache
     *
     * @param context - context
     * @throws IOException - if problem occurs while reading file from distributed cache
     */
    @Override
    protected void setup(Context context) throws IOException {

        cityMetaData = new HashMap<>();

        String fileName = "city.en.txt";
        Path path = Paths.get(fileName);
        Files.lines(path).forEach(this::addMetaData);
    }

    @Override
    protected void reduce(CompositeCity compositeCity, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //count total amount of values (bids)
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        bidsCount.set(sum);

        //get the name of the city by its id
        int cityId = compositeCity.getCityId();
        String name = cityMetaData.get(cityId);
        if (name != null) {
            cityName.set(name);
        } else {
            cityName.set("city_id_" + cityId);
        }

        context.write(cityName, bidsCount);
    }


    /**
     * Adds to the hash map values from the line -
     * city id as a key and its name as a value.
     *
     * @param line - line to add
     */
    private void addMetaData(String line) {

        String[] split = line.split("\\s");
        int cityId = Integer.parseInt(split[0].trim());
        String cityName = split[1].trim();
        cityMetaData.put(cityId, cityName);
    }

}
