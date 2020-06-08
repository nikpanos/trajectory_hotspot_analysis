package gr.unipi.ds.msc.analysis;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.unipi.ds.msc.common.AppConfig;
import gr.unipi.ds.msc.utils.accumulator.*;
import gr.unipi.ds.msc.utils.broadcast.Params;
import gr.unipi.ds.msc.utils.broadcast.Statistics;
import gr.unipi.ds.msc.utils.entity.Cell;
import gr.unipi.ds.msc.utils.entity.TrajectoryPoint;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.DoubleAccumulator;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.*;

public class PDatasetAnalysis {

    private static Broadcast<Params> doPreProcessingStep(JavaSparkContext sc, String inputPath, double cellSizeInDegrees, double timeStepSize, int outputNumber, long neighborDistance) {
        final SparkContext ssc = sc.sc();
        final MinimumLongAccumulator timeMin = new MinimumLongAccumulator(Long.MAX_VALUE, ssc);
        final MaximumLongAccumulator timeMax = new MaximumLongAccumulator(Long.MIN_VALUE, ssc);
        final MaximumDoubleAccumulator xMax = new MaximumDoubleAccumulator(Double.MIN_VALUE, ssc);
        final MaximumDoubleAccumulator yMax = new MaximumDoubleAccumulator(Double.MIN_VALUE, ssc);
        final MinimumDoubleAccumulator xMin = new MinimumDoubleAccumulator(Double.MAX_VALUE, ssc);
        final MinimumDoubleAccumulator yMin = new MinimumDoubleAccumulator(Double.MAX_VALUE, ssc);

        sc.textFile(inputPath).foreach((line) -> {
            TrajectoryPoint p = new TrajectoryPoint(line);
            if (p.isValidTrajectoryPoint()) {
                xMin.add(p.getLongitude());
                xMax.add(p.getLongitude());
                yMax.add(p.getLatitude());
                yMin.add(p.getLatitude());
                timeMin.add(p.getTimestamp());
                timeMax.add(p.getTimestamp());
            }
        });

        final Params params = new Params(timeMin.value(), timeMax.value(), cellSizeInDegrees,
                timeStepSize, xMin.value(), xMax.value(), yMin.value(), yMax.value(), outputNumber, neighborDistance);
        return sc.broadcast(params);
    }

    private static JavaPairRDD<Long, Double> calculateAttributeValuesSolution1(JavaSparkContext sc, String inputPath, Broadcast<Params> params) {
        JavaRDD<String> fileRDD = sc.textFile(inputPath);

        return fileRDD.flatMapToPair((line) -> {
            ArrayList<Tuple2<String, Tuple2<Long, Long>>> arr = new ArrayList<>(1);
            TrajectoryPoint p = new TrajectoryPoint(line);
            if (p.isValidTrajectoryPoint()) {
                String cellId = Long.toString(Cell.getCellIdFromTrajectoryPoint(p, params.value()));
                String vesselId = p.getVesselId();
                long timestampL = p.getTimestamp();
                long timestampH = p.getTimestamp();
                arr.add(new Tuple2<>(cellId + "-" + vesselId, new Tuple2<>(timestampL, timestampH)));
            }
            return arr.iterator();
        }).reduceByKey((v1, v2) -> {
            Long min = (v1._1 < v2._1) ? v1._1 : v2._1;  //keep the minimum of the timestampL values
            Long max = (v1._2 > v2._2) ? v1._2 : v2._2;  //keep the maximum of the timestampH values
            return new Tuple2<>(min, max);
        }).mapToPair((v1) -> {
            Long cellId = Long.parseLong(v1._1.split("-")[0]);  //keep only the vesselId
            double vesselTimeRatio = (v1._2._2 - v1._2._1) / params.value().dateStep; //i.e. (timestampH - timestampL) / dateStep
            return new Tuple2<>(cellId, vesselTimeRatio);
        }).reduceByKey((v1, v2) ->  v1 + v2 );
    }

    private static JavaPairRDD<Long, Double> calculateAttributeValuesSolution2(JavaSparkContext sc, String inputPath, Broadcast<Params> params) {
        JavaRDD<String> fileRDD = sc.textFile(inputPath);

        return fileRDD.flatMapToPair((line) -> {
            ArrayList<Tuple2<Long, Tuple2<String, Long>>> arr = new ArrayList<>(1);
            TrajectoryPoint p = new TrajectoryPoint(line);
            if (p.isValidTrajectoryPoint()) {
                Long cellId = Cell.getCellIdFromTrajectoryPoint(p, params.value());
                String vesselId = p.getVesselId();
                long timestamp = p.getTimestamp();
                arr.add(new Tuple2<>(cellId, new Tuple2<>(vesselId, timestamp)));
            }
            return arr.iterator();
        }).partitionBy(new CellPartitioner(params.value())
        ).mapPartitionsToPair((tuple2Iterator) -> {
            HashMap<String, Tuple2<Long, Long>> map = new HashMap<>();
            Long cellId, timestampL, timestampH;
            Tuple2<Long, Long> val;
            Tuple2<Long, Tuple2<String, Long>> next = null;
            while (tuple2Iterator.hasNext()) {
                next = tuple2Iterator.next();
                val = map.get(next._2._1);
                if (val == null) {
                    map.put(next._2._1, new Tuple2<>(next._2._2, next._2._2));
                }
                else {
                    timestampL = (next._2._2 < val._1) ? next._2._2 : val._1;
                    timestampH = (next._2._2 > val._2) ? next._2._2 : val._2;
                    map.put(next._2._1, new Tuple2<>(timestampL, timestampH));
                }
            }

            cellId = next._1;
            LinkedList<Tuple2<Long, Double>> arr = new LinkedList<>();
            for (Map.Entry<String, Tuple2<Long, Long>> e : map.entrySet()) {
                arr.push(new Tuple2<>(cellId, (e.getValue()._2 - e.getValue()._1) / params.value().dateStep));
            }
            return arr.iterator();
        });
    }

    private static Broadcast<Statistics> calculateStatistics(JavaSparkContext sc, JavaPairRDD<Long, Double> attributeValuesRDD, Params params) {
        final SparkContext ssc = sc.sc();
        final DoubleAccumulator accum1 = ssc.doubleAccumulator();
        final DoubleAccumulator accum2 = ssc.doubleAccumulator();

        attributeValuesRDD.foreach((t) -> {
            accum1.add(t._2);
            accum2.add(t._2 * t._2);
        });

        Statistics statistics = new Statistics(accum1.value(), accum2.value(), params);
        return sc.broadcast(statistics);
    }

    private static JavaPairRDD<Double, Long> calculateGetisOrd(JavaPairRDD<Long, Double> attributeValuesRDD, Broadcast<Statistics> broadcastStatistics, Broadcast<Params> broadcastParams) {
        return attributeValuesRDD.flatMapToPair((cKeyPair) ->
                new Cell(cKeyPair, broadcastParams.value()).getNeighborWeightedAttributeValuesList().iterator()
        ).reduceByKey((v1, v2) ->
                v1 + v2
        ).mapToPair((cKeyPair) ->
                new Cell(cKeyPair, broadcastParams.value()).calculateGetisOrd(broadcastStatistics.value())
        );
    }

    private static String makeJsonOutputStr(Params params, List<Tuple2<Double, Long>> result) {
        SimpleDateFormat outputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Config conf = AppConfig.getConfig();
        StringBuilder jsonRes = new StringBuilder("{");

        HashMap<String, String> generalInfo = new HashMap<>();
        Date execStart = new Date();
        execStart.setTime(params.executionStart);
        Date execEnd = new Date();
        execEnd.setTime(params.executionEnd);
        generalInfo.put("executionStart", outputDateFormat.format(execStart));
        generalInfo.put("executionEnd", outputDateFormat.format(execEnd));
        generalInfo.put("requestId", params.requestId + "");

        HashMap<String, String> paramsInfo = new HashMap<>();
        paramsInfo.put("k", params.outputNumber + "");
        paramsInfo.put("h", params.neighborDistance + "");
        paramsInfo.put("spatialCellSizeInDegrees", params.cellSize + "");
        paramsInfo.put("temporalCellSizeInDays", (params.dateStep / Params.dayInMillis) + "");
        String inputType = conf.getString("input.source");
        paramsInfo.put("datasourceDevice", inputType);
        paramsInfo.put("datasourceFormat", conf.getString("input.type"));
        if (inputType.equals("file")) {
            paramsInfo.put("datasourcePath", conf.getString("input.path"));
        }
        else if (inputType.equals("kafka")) {
            paramsInfo.put("kafkaTopic", conf.getString("input.kafkaTopic"));
        }

        jsonRes.append("\"generalInfo\":").
                append(makePlainJsonStringFromMap(generalInfo)).
                append(",\"params\":").
                append(makePlainJsonStringFromMap(paramsInfo)).
                append(",\"outputResult\": [");

        int i = 0;
        for (Tuple2<Double, Long> t : result) {
            HashMap<String, String> outputMap = new HashMap<>();
            Cell c = new Cell(new Tuple2<>(t._2, t._1), params);
            outputMap.put("polygonWKT", c.getPolygonWkt());
            outputMap.put("temporalStart", c.getTemporalStartStr(outputDateFormat));
            outputMap.put("temporalEnd", c.getTemporalEndStr(outputDateFormat));
            outputMap.put("zScore", t._1 + "");
            jsonRes.append(makePlainJsonStringFromMap(outputMap));
            if (++i != result.size()) {
                jsonRes.append(',');
            }
        }

        jsonRes.append("]}");
        return jsonRes.toString();
    }

    private static String makePlainJsonStringFromMap(Map<String, String> map) {
        StringBuilder builder = new StringBuilder("{");
        for (Map.Entry<String, String> kv: map.entrySet()) {
            builder.append('\"');
            builder.append(kv.getKey());
            builder.append("\":\"");
            builder.append(kv.getValue());
            builder.append("\",");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append('}');
        return builder.toString();
    }

    public static void analyze(String inputPath, String outputPath, Params params, SparkSession spark) throws IOException {
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
        BufferedWriter output = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputPath)), "UTF-8"));
        try {
            if (inputPath.charAt(inputPath.length() - 1) != '/') {
                inputPath += '/';
            }
            inputPath += '*';

            Broadcast<Params> broadcastParams = sc.broadcast(params);

            JavaPairRDD<Long, Double> attributeValuesRDD = calculateAttributeValuesSolution1(sc, inputPath, broadcastParams);

            Broadcast<Statistics> broadcastStatistics = calculateStatistics(sc, attributeValuesRDD, broadcastParams.value());

            JavaPairRDD<Double, Long> zScoresRDD = calculateGetisOrd(attributeValuesRDD, broadcastStatistics, broadcastParams);

            List<Tuple2<Double, Long>> result = zScoresRDD.sortByKey(false).take(params.outputNumber);

            params.executionEnd = System.currentTimeMillis();

            output.write(makeJsonOutputStr(params, result));

        } finally {
            output.close();
        }
    }
}
