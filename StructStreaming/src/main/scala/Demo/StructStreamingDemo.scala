package Demo

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
 * @author 虚竹
 * @date 2021/5/13 9:53
 * @version 1.0
 */
object StructStreamingDemo {
    def main(args: Array[String]): Unit = {


        val spark: SparkSession = SparkSession
            .builder
            .appName("StructStreamingDemo")
            .master("local[*]")
            .getOrCreate()


        import spark.implicits._

        val line: DataFrame = spark.readStream
            .format("socket")
            .option("host", "ddc001.lqad")
            .option("port", "9999")
            .load()
        val word: Dataset[String] = line.as[String].flatMap(_.split(" "))

        val wordCounts: DataFrame = word.groupBy("value").count()

        val query: StreamingQuery = wordCounts.writeStream
            .outputMode("complete")
            .format("console")
            .start()

        query.awaitTermination()
    }
}
