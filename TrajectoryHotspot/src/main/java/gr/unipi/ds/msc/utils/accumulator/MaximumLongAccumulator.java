package gr.unipi.ds.msc.utils.accumulator;

import org.apache.spark.SparkContext;
import org.apache.spark.util.AccumulatorV2;

public class MaximumLongAccumulator extends AccumulatorV2<Long, Long> {

    private Long myValue = new Long(0L);

    public MaximumLongAccumulator() {
    }

    public MaximumLongAccumulator(long initialValue, SparkContext sc, String name) {
        this.myValue = initialValue;
        sc.register(this, name);
    }

    public MaximumLongAccumulator(long initialValue, SparkContext sc) {
        this.myValue = initialValue;
        sc.register(this);
    }

    @Override
    public boolean isZero() {
        return myValue.longValue() == 0L;
    }

    @Override
    public AccumulatorV2<Long, Long> copy() {
        MaximumLongAccumulator res = new MaximumLongAccumulator();
        res.myValue = this.myValue;
        return res;
    }

    @Override
    public void reset() {
        myValue = new Long(0);
    }

    @Override
    public void add(Long v) {
        if (v > myValue) {
            myValue = v;
        }
    }

    @Override
    public void merge(AccumulatorV2<Long, Long> other) {
        if (other.value() > myValue) {
            myValue = other.value();
        }
    }

    @Override
    public Long value() {
        return myValue;
    }
}
