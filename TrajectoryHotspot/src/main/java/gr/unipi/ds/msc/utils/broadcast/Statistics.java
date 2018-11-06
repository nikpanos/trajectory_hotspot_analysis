package gr.unipi.ds.msc.utils.broadcast;

import java.io.Serializable;

/**
 * Class used for statistics calculations
 */
public class Statistics implements Serializable {
	private static final long serialVersionUID = -4963431329169308666L;

	public double attributeValueMean;
	public double attributeValueStandardDeviation;
	public long numberOfCellsInGrid;
	
	/**
	 * Statistics constructor
	 * 
	 * @param sum Total vessel count of grid
	 * @param squareSum vessel square sum of grid
	 * @param params A Params object that has been broadcasted to nodes in the cluster
	 */
	public Statistics(double sum, double squareSum, Params params) {
		numberOfCellsInGrid = (params.latitudeSegments * params.longitudeSegments * params.dateSegments);
		attributeValueMean = sum / ((double)numberOfCellsInGrid);
		double fraction = squareSum / ((double) numberOfCellsInGrid);
		attributeValueStandardDeviation = Math.sqrt(fraction - Math.pow(attributeValueMean, 2));
	}

	@Override
	public String toString() {
		return "Statistics [attributeValueMean=" + attributeValueMean + ", attributeValueStandardDeviation=" +
				attributeValueStandardDeviation + ", numberOfCellsInGrid=" + numberOfCellsInGrid + "]";
	}
	
}
