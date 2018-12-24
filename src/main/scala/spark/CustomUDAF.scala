package spark

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

class CustomUDAF extends UserDefinedAggregateFunction {
  // 指定输入数据的字段与类型
  override def inputSchema: StructType = DataTypes.createStructType(util.Arrays.asList(DataTypes.createStructField("cityInfo", DataTypes.StringType, true)))

  // 指定缓冲数据的字段与类型
  override def bufferSchema: StructType = DataTypes.createStructType(util.Arrays.asList(DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)))

  // 指定返回类型
  override def dataType: DataType = DataTypes.StringType

  // 指定是否是确定性的
  override def deterministic: Boolean = true

  /**
    * 初始化
    * 可以认为是，你自己在内部指定一个初始的值
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, "")
  }

  /**
    * 更新
    * 可以认为是，一个一个地将组内的字段值传递进来
    * 实现拼接的逻辑
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = { // 缓冲中的已经拼接过的城市信息串
    var bufferCityInfo: String = buffer.getString(0)
    // 刚刚传递进来的某个城市信息
    val cityInfo: String = input.getString(0)
    // 在这里要实现去重的逻辑
    // 判断：之前没有拼接过某个城市信息，那么这里才可以接下去拼接新的城市信息
    if (!bufferCityInfo.contains(cityInfo)) {
      if ("" == bufferCityInfo) bufferCityInfo += cityInfo
      else {
        // 比如1:北京
        // 1:北京,2:上海
        bufferCityInfo += "," + cityInfo
      }
      buffer.update(0, bufferCityInfo)
    }
  }

  /**
    * 合并
    * update操作，可能是针对一个分组内的部分数据，在某个节点上发生的
    * 但是可能一个分组内的数据，会分布在多个节点上处理
    * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferCityInfo1: String = buffer1.getString(0)
    val bufferCityInfo2: String = buffer2.getString(0)
    for (cityInfo <- bufferCityInfo2.split(",")) {
      if (!bufferCityInfo1.contains(cityInfo))
        if ("" == bufferCityInfo1)
          bufferCityInfo1 += cityInfo
        else bufferCityInfo1 += "," + cityInfo
    }
    buffer1.update(0, bufferCityInfo1)
  }

  override def evaluate(row: Row): Any = row.getString(0)
}
