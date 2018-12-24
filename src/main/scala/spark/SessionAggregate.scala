package spark

import java.util.Properties

import com.qf.sessionanalyze.constant.Constants
import com.qf.sessionanalyze.util.{NumberUtils, StringUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}


object SessionAggregate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION)
      .setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName(Constants.SPARK_APP_NAME_SESSION).getOrCreate()

    val url = "jdbc:mysql://localhost:3306/sessionanalyze?useUnicode=true&characterEncoding=utf8"
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    val tableName = "sessionBase"

    val rdd: RDD[Row] = spark.read.jdbc(url, tableName, props).rdd
    val sessionId2AggregateInfoRDD: RDD[String] = rdd.map(_.toString())
    // 先过滤出年龄在20-40之间的人的全量信息
    val filteredRDD: RDD[String] = FilterAnalyze.filterByAny(sessionId2AggregateInfoRDD, Constants.FIELD_AGE, "20-40")
    val accum = new SessionAccumulator()
    sc.register(accum, "customAccum")

    filteredRDD.foreach(str => {
      val visitLength = StringUtils.getFieldFromConcatString(str, "\\|", Constants.FIELD_VISIT_LENGTH)
      val stepLength = StringUtils.getFieldFromConcatString(str, "\\|", Constants.FIELD_STEP_LENGTH)
      accum.add(Constants.SESSION_COUNT)
      if (StringUtils.isNotEmpty(visitLength)) {
        calculateVisitLength(accum, visitLength.toLong / 1000)
      }
      if (StringUtils.isNotEmpty(stepLength)) {
        calculateStepLength(accum, stepLength.toLong)
      }
    })

    val str = getVisitOrStepPercentage(accum, "30m")
    println("访问时间在30m以上的占比为：" + str)
    val str2 = getVisitOrStepPercentage(accum, "10_30")
    println("访问步长在10到30之间的占比为：" + str2)
  }

  /**
    * 计算访问时长和步长的占比(自己实现...代补全：各阶段的占比并存储到数据库)
    * @param accum
    * @param visitOrStepScope
    * @return
    */
  def getVisitOrStepPercentage(accum: SessionAccumulator, visitOrStepScope: String): String = {
    val v = accum.value
    val strings = v.split("\\|")
    var num = 0.0
    for (elem <- strings) {
      val item = elem.split("=")
      if (visitOrStepScope.equals(item(0))) {
        num = item(1).toDouble
      }
    }
    NumberUtils.formatDouble((num / strings(0).split("=")(1).toDouble) * 100, 2) + "%"
  }

  //计算访问时长
  val calculateVisitLength = (accum: SessionAccumulator, visitLength: Long) => {
    if (visitLength >= 1 && visitLength <= 3)
      accum.add(Constants.TIME_PERIOD_1s_3s)
    else if (visitLength >= 4 && visitLength <= 6)
      accum.add(Constants.TIME_PERIOD_4s_6s)
    else if (visitLength >= 7 && visitLength <= 9)
      accum.add(Constants.TIME_PERIOD_7s_9s)
    else if (visitLength >= 10 && visitLength <= 30)
      accum.add(Constants.TIME_PERIOD_10s_30s)
    else if (visitLength > 30 && visitLength <= 60)
      accum.add(Constants.TIME_PERIOD_30s_60s)
    else if (visitLength > 60 && visitLength <= 180)
      accum.add(Constants.TIME_PERIOD_1m_3m)
    else if (visitLength > 180 && visitLength <= 600)
      accum.add(Constants.TIME_PERIOD_3m_10m)
    else if (visitLength > 600 && visitLength <= 1800)
      accum.add(Constants.TIME_PERIOD_10m_30m)
    else if (visitLength > 1800)
      accum.add(Constants.TIME_PERIOD_30m)
  }
  //  计算步长
  val calculateStepLength = (accum: SessionAccumulator, stepLength: Long) => {
    if (stepLength >= 1 && stepLength <= 3)
      accum.add(Constants.STEP_PERIOD_1_3)
    if (stepLength >= 4 && stepLength <= 6)
      accum.add(Constants.STEP_PERIOD_4_6)
    if (stepLength >= 7 && stepLength <= 9)
      accum.add(Constants.STEP_PERIOD_7_9)
    if (stepLength >= 10 && stepLength <= 30)
      accum.add(Constants.STEP_PERIOD_10_30)
    if (stepLength >= 30 && stepLength <= 60)
      accum.add(Constants.STEP_PERIOD_30_60)
    if (stepLength > 60)
      accum.add(Constants.STEP_PERIOD_60)
  }

}
