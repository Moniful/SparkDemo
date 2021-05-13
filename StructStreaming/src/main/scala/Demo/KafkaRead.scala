package Demo

import java.time.LocalDateTime

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
 * @author 虚竹
 * @date 2021/5/13 13:36
 * @version 1.0
 */
object KafkaRead {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
            .builder
            .appName("KafkaRead")
            .master("local[*]")
            .getOrCreate()

        import spark.implicits._
        val df: DataFrame = spark.readStream
            .format("kafka")
            //kafka集群
            .option("kafka.bootstrap.servers", "ddc001.lqad:9092,ddc002.lqad:9092,ddc003.lqad:9092,ddc004.lqad:9092,ddc005.lqad:9092")
            //kafka topic
            .option("subscribe", "xzhang")
            //kafka 组号 便于归类，不一定需要
            .option("group.id", "leqee")
            //从头消费
            .option("startingOffsets", "earliest")
            .load()

        //        val query: StreamingQuery = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        //            .as[(String, String)]
        //            .writeStream
        //            .format("console").outputMode("append").start()
        //        query.awaitTermination()

        val frame: DataFrame = df.select("value").as[String]
            .map(s => {
                val t = s.split(" ")
                (t(0), t(1).toDouble)
            }).toDF("name", "score")
//        val query: StreamingQuery = frame.writeStream.format("console").outputMode("append").start()


        val query: StreamingQuery = frame
            .writeStream
            .queryName("demo")
            .foreachBatch { (batchDF: DataFrame, _: Long) => {
                batchDF.persist()


                println(LocalDateTime.now() + "start writing mor table")
                batchDF.write.format("org.apache.hudi")
                    .option("TABLE_TYPE_OPT_KEY", "MERGE_ON_READ")
                    .option("TABLE_TYPE_OPT_KEY", "COPY_ON_WRITE")
                    .option("PRECOMBINE_FIELD_OPT_KEY", "name")
                    .option("RECORDKEY_FIELD_OPT_KEY", "name")
                    .option("TABLE_NAME", "demo")
                    .mode(SaveMode.Append)
                    .save("hdfs://ddc002.lqad:8020/tmp/xzhang/MERGE_ON_READ")

                println(LocalDateTime.now() + "finish")
                batchDF.unpersist()
            }
            }
            .option("checkpointLocation", "/tmp/sparkHudi/checkpoint/")
            .start()





        query.awaitTermination()

    }

}
