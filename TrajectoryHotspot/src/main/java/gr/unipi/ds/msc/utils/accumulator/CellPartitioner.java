package gr.unipi.ds.msc.utils.accumulator;

import gr.unipi.ds.msc.utils.broadcast.Params;
import org.apache.spark.Partitioner;

public class CellPartitioner extends Partitioner {

    private Params params;

    public CellPartitioner(Params params) {
        this.params = params;
    }

    @Override
    public int numPartitions() {
        return (int) (params.longitudeSegments * params.latitudeSegments * params.dateSegments);
    }

    @Override
    public int getPartition(Object key) {
        Long value = (Long)key;
        return value.intValue();
    }
}
