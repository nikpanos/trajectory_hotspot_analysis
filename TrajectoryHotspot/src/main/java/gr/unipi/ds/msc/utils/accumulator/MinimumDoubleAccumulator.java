package gr.unipi.ds.msc.utils.accumulator;

import org.apache.spark.SparkContext;
import org.apache.spark.util.AccumulatorV2;

public class MinimumDoubleAccumulator extends AccumulatorV2<Double, Double> {

	private Double myValue = new Double(0);

	public MinimumDoubleAccumulator() {
	}

	public MinimumDoubleAccumulator(double initialValue, SparkContext sc, String name) {
		this.myValue = initialValue;
		sc.register(this, name);
	}

	public MinimumDoubleAccumulator(double initialValue, SparkContext sc) {
		this.myValue = initialValue;
		sc.register(this);
	}

	@Override
	public boolean isZero() {
		return myValue.doubleValue() == 0D;
	}

	@Override
	public AccumulatorV2<Double, Double> copy() {
		MinimumDoubleAccumulator res = new MinimumDoubleAccumulator();
		res.myValue = this.myValue;
		return res;
	}

	@Override
	public void reset() {
		myValue = new Double(0);
	}

	@Override
	public void add(Double v) {
		if (v < myValue) {
			myValue = v;
		}
	}

	@Override
	public void merge(AccumulatorV2<Double, Double> other) {
		if (other.value() < myValue) {
			myValue = other.value();
		}
	}

	@Override
	public Double value() {
		return myValue;
	}
}
