package Demo

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author 虚竹
 * @date 2021/5/14 16:38
 * @version 1.0
 */
object ReadParquet {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder
            .appName("KafkaRead")
            .master("local[*]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        val df: DataFrame = spark.read.parquet("F:\\Demo\\default\\*.parquet")


        df.show()

        df.select("*").show()




    }

}
