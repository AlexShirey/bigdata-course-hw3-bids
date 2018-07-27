package bigdata.course.hw3.bids;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * The custom writable class that is used as a key
 */
public class CompositeCity implements WritableComparable<CompositeCity> {

    static {
        WritableComparator.define(CompositeCity.class, new Comparator());
    }

    private int cityId;
    private String operationSystem;

    public CompositeCity() {
    }

    public CompositeCity(int cityId, String operationSystem) {
        this.cityId = cityId;
        this.operationSystem = operationSystem;
    }

    public int getCityId() {
        return cityId;
    }

    public void setCityId(int cityId) {
        this.cityId = cityId;
    }

    public String getOperationSystem() {
        return operationSystem;
    }

    public void setOperationSystem(String operationSystem) {
        this.operationSystem = operationSystem;
    }

    /**
     * Since CompositeCity is used as a key,
     * this method is necessary for MR framework for sorting.
     * <p>
     * Sort keys only by cityId:
     * - useful for Combiner (he will receive all values for certain city id and aggregate them, so Reducer will receive less records)
     * - no need to use GroupingComparator to group by city if we have custom Partitioner (e.g. by operation system)
     *
     * @param o the object to be compared.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     * @throws NullPointerException if the specified object is null
     */
    @Override
    public int compareTo(CompositeCity o) {

        int otherCityId = o.getCityId();
        return Integer.compare(cityId, otherCityId);
    }

    /**
     * The implementation of the Raw Comparator.
     * Compares only by city id.
     */
    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(CompositeCity.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            int cityId1 = readInt(b1, s1);
            int cityId2 = readInt(b2, s2);

            return Integer.compare(cityId1, cityId2);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(cityId);
        out.writeUTF(operationSystem);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        cityId = in.readInt();
        operationSystem = in.readUTF();
    }

    /**
     * This method is used by default HashCodePartitioner.
     * <p>
     * Counts hashcode only by city id,
     * as we need that only one reducer obtains all occurrences for certain city
     *
     * @return a hash code value for this object
     */
    @Override
    public int hashCode() {

        int result = 1;
        result = 31 * result + cityId;
        return result;
    }

    //used in test only
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeCity that = (CompositeCity) o;
        return cityId == that.cityId &&
                Objects.equals(operationSystem, that.operationSystem);
    }
}
