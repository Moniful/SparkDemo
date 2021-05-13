package Demo

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author 虚竹
 * @date 2021/5/13 13:36
 * @version 1.0
 */
object KafkaWrite {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder
            .appName("KafkaWrite")
            .master("local[*]")
            .getOrCreate()

        val df: DataFrame = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "ddc001.lqad:9092,ddc002.lqad:9092,ddc003.lqad:9092,ddc004.lqad:9092,ddc005.lqad:9092")
            .option("subscribe", "xzhang")
            .load()

        import spark.implicits._
        val query: StreamingQuery = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            .writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "ddc001.lqad:9092,ddc002.lqad:9092,ddc003.lqad:9092,ddc004.lqad:9092,ddc005.lqad:9092")
            .option("checkpointLocation", "./ck1")
            .start()
        query.awaitTermination()



    }

}
