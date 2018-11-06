
package gr.unipi.ds.msc.utils.entity;

import gr.unipi.ds.msc.utils.broadcast.Params;
import gr.unipi.ds.msc.utils.broadcast.Statistics;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Cell {
    private long id;

	private long x;
	private long y;
	private long t;

	private double attributeValue;
    private Params params;

	public Cell(Tuple2<Long, Double> cellTuple, Params params) {
	    id = cellTuple._1;
		t = id / (params.latitudeSegments * params.longitudeSegments);
		id -= t * params.latitudeSegments * params.longitudeSegments;
		y = id / params.longitudeSegments;
		x = id - y * params.longitudeSegments;

		attributeValue = cellTuple._2;
		this.params = params;
	}

	private static long convertCellComponentsToId(long x, long y, long t, Params params) {
		return (x + y * params.longitudeSegments + t * params.longitudeSegments * params.latitudeSegments);
	}

	public static long getCellIdFromTrajectoryPoint(TrajectoryPoint p, Params params) {
        long x = (long) ((p.getLongitude() - params.longitudeMin) / params.cellSize);
        long y = (long) ((p.getLatitude() - params.latitudeMin) / params.cellSize);
        long t = (long) ((p.getTimestamp() - params.dateMin) / params.dateStep);

        return convertCellComponentsToId(x, y, t, params);
    }

	public List<Tuple2<Long, Double>> getNeighborWeightedAttributeValuesList() {
        List<Tuple2<Long, Double>> result = new ArrayList<>();
        long neighborDistance = params.neighborDistance;

        for (long i = x - neighborDistance; i <= x + neighborDistance; i++) {
            if ((i < 0) || (i >= params.longitudeSegments))
                continue;
            for (long j = y - neighborDistance; j <= y + neighborDistance; j++) {
                if ((j < 0) || (j >= params.latitudeSegments))
                    continue;
                for (long k = t - neighborDistance; k <= t + neighborDistance; k++) {
                    if ((k < 0) || (k >= params.dateSegments))
                        continue;
                    double power = Math.max(Math.max(Math.abs(x-i), Math.abs(j-y)), Math.abs(k-t));
                    if (power > 0) {
                        power = power - 1;
                    }
                    result.add(new Tuple2<>(id, attributeValue * Math.pow(Params.weightFactor, -power)));
                }
            }
        }
        return result;
    }

    private Tuple2<Double, Double> getSumNeighborWeightsAndSquared() {
        double sum = 0d, sum2 = 0d;
        long neighborDistance = params.neighborDistance;

        for (long i = x - neighborDistance; i <= x + neighborDistance; i++) {
            if ((i < 0) || (i >= params.longitudeSegments))
                continue;
            for (long j = y - neighborDistance; j <= y + neighborDistance; j++) {
                if ((j < 0) || (j >= params.latitudeSegments))
                    continue;
                for (long k = t - neighborDistance; k <= t + neighborDistance; k++) {
                    if ((k < 0) || (k >= params.dateSegments))
                        continue;
                    double power = Math.max(Math.max(Math.abs(x-i), Math.abs(j-y)), Math.abs(k-t));
                    if (power > 0) {
                        power = power - 1;
                    }
                    sum += Math.pow(Params.weightFactor, -power);
                    sum2 += Math.pow(Params.weightFactor, - 2 * power);
                }
            }
        }

        return new Tuple2<>(sum, sum2);
    }

    public Tuple2<Double, Long> calculateGetisOrd(Statistics statistics) {
	    Tuple2<Double, Double> tmpSum = getSumNeighborWeightsAndSquared();
	    double weightSum = tmpSum._1;
	    double weightSquaredSum = tmpSum._2;

	    double numerator = attributeValue - statistics.attributeValueMean * weightSum;
	    double fraction = (((double)statistics.numberOfCellsInGrid) * weightSquaredSum - Math.pow(weightSum, 2)) / ((double)(statistics.numberOfCellsInGrid - 1));
	    double denominator = statistics.attributeValueStandardDeviation * Math.sqrt(fraction);
	    return new Tuple2<>(numerator / denominator, id);
    }

	public long getX() {
		return x;
	}

	public long getY() {
		return y;
	}

	public long getT() {
		return t;
	}

/*
    private double getSumNeighborWeightsNaive() {
	    double sum = 0d;
        long neighborDistance = params.neighborDistance;

        int[] counts = new int[10];

        for (long i = x - neighborDistance; i <= x + neighborDistance; i++) {
            if ((i < 0) || (i >= params.longitudeSegments))
                continue;
            for (long j = y - neighborDistance; j <= y + neighborDistance; j++) {
                if ((j < 0) || (j >= params.latitudeSegments))
                    continue;
                for (long k = t - neighborDistance; k <= t + neighborDistance; k++) {
                    if ((k < 0) || (k >= params.dateSegments))
                        continue;
                    double power = Math.max(Math.max(Math.abs(x-i), Math.abs(j-y)), Math.abs(k-t));
                    if (power > 0) {
                        power = power - 1;
                    }
                    counts[(int)power]++;
                    sum += Math.pow(Params.weightFactor, -power);
                }
            }
        }

        for (int i = 0; i < counts.length; i++) {
            System.out.printf("counts[%d]=%d\n", i, counts[i]);
        }

        return sum;
    }

    private double getSumNeighborWeightsClever() {
        double sum = 0d;
        long neighborDistance = params.neighborDistance;
        long cellId = convertCellComponentsToId(this.x, this.y, this.t, params);

        long cellCount;

        for (int i = 0; i <= neighborDistance; i++) {
            if (i == 0) {
                sum += 1;
            }
            else {
                cellCount = 24 * i * i + 2; //This formula is only for 3D
                sum += cellCount * Math.pow(Params.weightFactor,  1 - i);
            }
        }
        return sum;
    }

    public static void main(String[] args) {
        Params params = new Params(0, 10, 0.1, 0.1 / Params.dayInMillis, 0, 10, 0, 10, 50, 2);
        Cell c = new Cell(new Tuple2<>(20200L, 0d), params);

        System.out.printf("x=%d\ny=%d\nt=%d\n", c.x, c.y, c.t);
        System.out.printf("longitude segments=%d\nlatitude segments=%d\ntime segments=%d\n", params.longitudeSegments, params.latitudeSegments, params.dateSegments);

        double clever = c.getSumNeighborWeightsClever();
        double naive = c.getSumNeighborWeightsNaive();

        String result;
        if (clever == naive) {
            result = "Success!";
        }
        else {
            result = "Fail!";
        }

        System.out.printf("%s\nClever = %f\nNaive = %f\n", result, clever, naive);
    }
*/
}
