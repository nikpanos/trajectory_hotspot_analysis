package gr.unipi.ds.msc

import gr.unipi.ds.msc.common.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.typesafe.config.Config
import gr.unipi.ds.msc.utils.broadcast.Params
import org.apache.spark.broadcast.Broadcast

import collection.JavaConverters._

object Preprocess {

  def getAllConfigsStartingWith(conf: Config, prefix: String): Map[String, String] = {
    conf.entrySet().asScala.map(x => (x.getKey, x.getValue.unwrapped().toString)).filter(x => x._1.startsWith(prefix)).toMap
  }

  def doPreprocess(spark: SparkSession, outName: String): Params = {
    val conf = AppConfig.getConfig
    val df = conf.getString("input.source") match {
      case "file" =>
        conf.getString("input.type") match {
          case "json" => spark.read.json(conf.getString("input.path"))
        }
      case "kafka" =>
        val topicName = conf.getString("input.kafkaTopic")
        val extraConfs = getAllConfigsStartingWith(conf, "kafka.")
        spark
          .read
          .format("kafka")
          .options(extraConfs)
          .option("subscribe", topicName)
          .option("startingOffsets", "earliest")
          .load()
        //TODO: fix data types here
    }
    val df1 = df.filter(col(conf.getString("input.roadIdFieldName")) =!= -1).select(
      col(conf.getString("input.timestampFieldName")).alias("tstamp"),
      col(conf.getString("input.vehicleFieldName")).alias("vehicle"),
      col(conf.getString("input.latitudeFieldName")).alias("lat"),
      col(conf.getString("input.longitudeFieldName")).alias("lon")
    )
    val timestampFormat = conf.getString("input.timestampFormat")
    val df2 = if (timestampFormat.startsWith("STR=")) {
      val format = timestampFormat.substring(4)
      df1.withColumn("tstamp",
        unix_timestamp(col("tstamp"), format))
    }
    else {
      df1
    }
    val stats = df2.select(
      (min(col("tstamp")) * 1000L).alias("minTime"),
      (max(col("tstamp")) * 1000L).alias("maxTime"),
      min(col("lat")).alias("minLat"),
      max(col("lat")).alias("maxLat"),
      min(col("lon")).alias("minLon"),
      max(col("lon")).alias("maxLon")
    ).collect().head
    val params = new Params(
      stats.getAs[Long]("minTime"),
      stats.getAs[Long]("maxTime"),
      conf.getDouble("process.spatialCellSizeInDegrees"),
      conf.getDouble("process.temporalCellSizeInDays"),
      stats.getAs[Double]("minLon"),
      stats.getAs[Double]("maxLon"),
      stats.getAs[Double]("minLat"),
      stats.getAs[Double]("maxLat"),
      conf.getInt("process.k"),
      conf.getInt("process.h"))
    df2.printSchema()
    df2.write.option("delimiter", "\t").option("header", false).csv(outName)
    params
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("a").getOrCreate();
    AppConfig.initiate(null)
    doPreprocess(spark, "aa")
  }
}