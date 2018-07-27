package bigdata.course.hw3.bids.util;

import eu.bitwalker.useragentutils.UserAgent;

/**
 * The util class that is used to parse records from the dataset and
 * to get user's os type with UserAgent lib help
 */
public class RecordParser {

    private int cityId;
    private int bidPrice;
    private String operationSystem;

    /**
     * Checks the record for validity and parses it
     *
     * @param record - record to parse
     * @return - true, if the record is valid, false otherwise
     */
    public boolean parseRecord(String record) {

        if (record.trim().length() == 0) {
            return false;
        }

        String[] splittedRecord = record.split("\\t");

        try {
            cityId = Integer.parseInt(splittedRecord[7]);
            bidPrice = Integer.parseInt(splittedRecord[19]);
            operationSystem = UserAgent.parseUserAgentString(splittedRecord[4]).getOperatingSystem().toString();
        } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
            return false;
        }

        return true;
    }


    public int getCityId() {
        return cityId;
    }

    public int getBidPrice() {
        return bidPrice;
    }

    public String getOperationSystem() {
        return operationSystem;
    }
}
