package gr.unipi.ds.msc.utils.entity;

import gr.unipi.ds.msc.utils.broadcast.Params;
import scala.Tuple3;

public class TrajectoryPoint {
    private String vesselId;
    private double longitude;
    private double latitude;
    private long timestamp;

    private boolean validTrajectoryPoint = false;

    public TrajectoryPoint(String rawLine) {
        if (!rawLine.startsWith("o")) {
            String[] words = rawLine.split("\t"); //split the csv formatted line to get individual tokens

            if (words.length != 4) {
                throw new RuntimeException("");
            }
            validTrajectoryPoint = true;
            //Get vessel's phenomenon ending timestamp
            timestamp = Long.parseLong(words[0]) * 1000L;
            //Get vessel's unique identifier
            vesselId = words[1];
            //Get vessel's longitude coordinate
            longitude = Double.parseDouble(words[3]);
            //Get vessel's latitude coordinate
            latitude = Double.parseDouble(words[2]);
        }
    }

    public String getVesselId() {
        return vesselId;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isValidTrajectoryPoint() {
        return validTrajectoryPoint;
    }

}
