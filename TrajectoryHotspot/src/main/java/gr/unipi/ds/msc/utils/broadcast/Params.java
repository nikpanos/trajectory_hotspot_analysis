package gr.unipi.ds.msc.utils.broadcast;

import java.io.Serializable;

/**
 * Helper class calculating the bounding box for the hotspot analysis
 */
public class Params implements Serializable {
	private static final long serialVersionUID = 203031547741399818L;
	
	public static final double dayInMillis = 86400000d;	// length of a day in milliseconds
	public static final double weightFactor = 2d;
	public double longitudeMin;		// bounding box longitude min
	public double longitudeMax;		// bounding box longitude max
	public double latitudeMin;		// bounding box latitude min
	public double latitudeMax;		// bounding box latitude max
	public long dateMin;			// bounding box date min in milliseconds
	public long dateMax;			// bounding box date max in milliseconds
	public long dateSegments; 		// number of segments in grid for z dimension (date)
	public double dateStep; 		// size of a segment in z dimension
	public long longitudeSegments; 	// number of segments in grid for x dimension (longitude)
	public long latitudeSegments; 	// number of segments in grid for y dimension (latitude)
	public long altitudeSegments;	// number of segments in grid for z dimension (altitude)
	public double cellSize;			// size of a segment in x and y dimensions
	public int outputNumber;		// number of top-k cells found
	public long neighborDistance;   // size of neighborhood
	public long executionStart;
	public long executionEnd;
	public long requestId;
	
	/**
	 * {@link Params} first constructor.
	 * Used for vessel dataset
	 * 
	 * @param dateMin long primitive type for minimum date
	 * @param dateMax long primitive type for maximum date
	 * @param cellSize double primitive type for cell size in degrees
	 * @param timeStepSize double primitive type for cell time window
	 * @param longitudeMin double primitive type for minimum longitude
	 * @param longitudeMax double primitive type for maximum longitude
	 * @param latitudeMin double primitive type for minimum latitude
	 * @param latitudeMax double primitive type for maximum latitude
	 * @param outputNumber integer primitive type for top-k cells found
	 */
	public Params(long dateMin, long dateMax, double cellSize, double timeStepSize,
			double longitudeMin, double longitudeMax, double latitudeMin, double latitudeMax, int outputNumber, long neighborDistance) {
		this.longitudeMin = longitudeMin;
		this.longitudeMax = longitudeMax;
		this.latitudeMin = latitudeMin;
		this.latitudeMax = latitudeMax;
		this.dateMax = dateMax;
		this.dateMin = dateMin;
		dateStep = dayInMillis * timeStepSize;
		dateSegments = (long) Math.ceil((this.dateMax - this.dateMin) / dateStep);
		longitudeSegments = (long) Math.ceil((longitudeMax - longitudeMin) / cellSize);
		latitudeSegments = (long) Math.ceil((latitudeMax - latitudeMin) / cellSize);
		this.cellSize = cellSize;
		this.outputNumber = outputNumber;
		this.neighborDistance = neighborDistance;
	}

	/**
	 * Helper method to get a cells minimum timestamp in millisecond
	 * 
	 * @param cellDateSegment a long primitive type representing a cell's date
	 * segment
	 * @return cell's minimum timestamp
	 */
	public long getCellMinTimestamp(long cellDateSegment) {
		return (long) (dateMin + cellDateSegment * dateStep);
	}

	/**
	 * Helper method to get a cells maximum timestamp in millisecond
	 * 
	 * @param cellDateSegment a long primitive type representing a cell's date
	 * segment
	 * @return cell's maximum timestamp
	 */
	public long getCellMaxTimestamp(long cellDateSegment) {
		return (long) (dateMin + (cellDateSegment + 1) * dateStep);
	}
	
	@Override
	public String toString() {
		return "Params [longitudeMin=" + longitudeMin + ", longitudeMax=" + longitudeMax + ", latitudeMin="
				+ latitudeMin + ", latitudeMax=" + latitudeMax + ", dateMin=" + dateMin + ", dateMax=" + dateMax
				+ ", dateSegments=" + dateSegments + ", dateStep=" + dateStep + ", longitudeSegments="
				+ longitudeSegments + ", latitudeSegments=" + latitudeSegments + ", cellSize=" + cellSize + "]";
	}
}
