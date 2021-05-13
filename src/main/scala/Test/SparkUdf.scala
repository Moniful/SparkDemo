package Test

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}
import org.apache.spark.sql.types._


/**
 * @author 虚竹
 * @date 2021/5/6 17:09
 * @version 1.0
 */

object SparkUdf extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = {
        StructType(StructField("inputColumn", DoubleType) :: Nil)
    }

    override def bufferSchema: StructType = {
        StructType(StructField("sum", DoubleType) :: Nil)
    }

    override def dataType: DataType = {
        DoubleType
    }

    override def deterministic: Boolean = {
        true
    }

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0D
        buffer(1) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        if (!input.isNullAt(0)) {
            buffer(0) = buffer.getDouble(0) + input.getDouble(0)
            buffer(1) = buffer.getLong(1) + 1
        }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        if (!buffer2.isNullAt(0)) {
            buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
            buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
        }
    }

    override def evaluate(buffer: Row): Any = {
        println(buffer.getDouble(0), buffer.getLong(1))
        buffer.getDouble(0) / buffer.getLong(1)

    }
}
