package Test

import com.sun.prism.PixelFormat.DataType
import org.apache.spark.sql.SparkSession

/**
 * @author 虚竹
 * @date 2021/5/6 14:48
 * @version 1.0
 */
object SaprkSqlTest {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName("test")
            .getOrCreate()

        spark.udf.register("myUdf", SparkUdf)

        val df = spark
            .read
            .json("E:\\a.json")
        df.show()
        df.createTempView("ggg")

//        spark.sql(
//            """
//              |select name ,salary
//              |from ggg
//              |where salary >3800
//              |""".stripMargin)
//            .show()

        spark.sql(
            """
              |select myUdf(salary) avg_salary
              |from ggg
              |""".stripMargin)
                .show()

        spark.stop()

    }
}
