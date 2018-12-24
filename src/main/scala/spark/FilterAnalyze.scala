package spark

import java.util.Properties
import com.qf.sessionanalyze.constant.Constants
import com.qf.sessionanalyze.util.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

object FilterAnalyze {
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

    val resRDD = filterByAny(sessionId2AggregateInfoRDD, Constants.FIELD_SEARCH_KEYWORDS, "日本料理,蛋糕")
    //    resRDD.collect().foreach(println)

    val res1RDD = filterByAny(sessionId2AggregateInfoRDD, Constants.FIELD_AGE, "28-35")
    //    res1RDD.collect().foreach(println)

    val res2RDD = filterByAny(sessionId2AggregateInfoRDD, Constants.FIELD_CITY, "city82")
    //    res2RDD.collect().foreach(println)

    val res3RDD = filterByAny(sessionId2AggregateInfoRDD, Constants.FIELD_SEX, "male")
    //    res3RDD.collect().foreach(println)

    val res4RDD = filterByAny(sessionId2AggregateInfoRDD, Constants.FIELD_PROFESSIONAL, "professional28,professional21")
    //    res4RDD.collect().foreach(println)

    val res5RDD = filterByAny(sessionId2AggregateInfoRDD, Constants.FIELD_START_TIME, "2018-12-11 11:00:00,2018-12-11 13:00:00")
    //        res5RDD.collect().foreach(println)

    val resAllRDD = filterByAny(sessionId2AggregateInfoRDD,
      Constants.FIELD_SEARCH_KEYWORDS + "=日本料理,蛋糕|" +
        Constants.FIELD_AGE + "=28-38|" +
        Constants.FIELD_CITY + "=city64|" +
        Constants.FIELD_SEX + "=male|" +
        Constants.FIELD_PROFESSIONAL + "=professional28,professional85|" +
        Constants.FIELD_START_TIME + "=2018-12-11 08:00:00,2018-12-11 13:00:00"
    )
    resAllRDD.collect().foreach(println)
  }

  /**
    * 根据传入的key和传入的条件进行过滤
    *
    * @param sessionAndUserInfo 要过滤的RDD
    * @param arg                按此关键字进行过滤
    * @return
    */
  def filterByAny(sessionAndUserInfo: RDD[String], arg: Any*): RDD[String] = {
    var filteredRDD: RDD[String] = sessionAndUserInfo
    var key: String = null
    var value: Any = null
    if (arg.length == 2) {
      key = arg(0).asInstanceOf[String]
      value = arg(1)
      filteredRDD = matchDef(filteredRDD, key, value)
    }
    else if (arg.length == 1) {
      val strings: Array[String] = arg(0).asInstanceOf[String].split("\\|")
      for (elem <- strings) {
        val keyValue: Array[String] = elem.split("=")
        key = keyValue(0)
        value = keyValue(1)
        filteredRDD = matchDef(filteredRDD, key, value)
      }
    }
    filteredRDD
  }

  /**
    * 匹配方法
    *
    * @param sessionAndUserInfo
    * @param key
    * @param value
    * @return
    */
  def matchDef(sessionAndUserInfo: RDD[String], key: String, value: Any): RDD[String] = {
    var filteredRDD: RDD[String] = sessionAndUserInfo
    key match {
      // 判断关键字
      case Constants.FIELD_SEARCH_KEYWORDS => {
        val keywordsArr = value.asInstanceOf[String].split(",").toList
        filteredRDD = sessionAndUserInfo.filter(str => {
          var bool = true
          if (StringUtils.isNotEmpty(StringUtils.getFieldFromConcatString(str, "\\|", key))) {
            val ss = StringUtils.getFieldFromConcatString(str, "\\|", key).split(",").toList
            keywordsArr.foreach {
              x => {
                if (!ss.contains(x)) {
                  bool = false
                }
              }
            }
          }
          bool && StringUtils.isNotEmpty(StringUtils.getFieldFromConcatString(str, "\\|", key))
        })
      }
      // 匹配年龄的范围
      case Constants.FIELD_AGE => {
        val tuple = value.asInstanceOf[String].split("-")
        if (tuple(0).toInt <= tuple(1).toInt) {
          filteredRDD = sessionAndUserInfo.filter(str => {
            val age = StringUtils.getFieldFromConcatString(str, "\\|", key).toInt
            age >= tuple(0).toInt && age <= tuple(1).toInt
          })
        } else {
          println("输入的年龄范围不合法!")
        }
      }
      // 匹配城市或性别
      case Constants.FIELD_CITY | Constants.FIELD_SEX => {
        val vl = value.asInstanceOf[String]
        filteredRDD = sessionAndUserInfo.filter(str => {
          val getVl = StringUtils.getFieldFromConcatString(str, "\\|", key)
          StringUtils.isNotEmpty(getVl) && org.apache.commons.lang.StringUtils.equals(getVl, vl)
        })
      }
      // 匹配职业在某些范围内的
      case Constants.FIELD_PROFESSIONAL => {
        val proArr = value.asInstanceOf[String].split(",")
        filteredRDD = sessionAndUserInfo.filter(str => {
          val getPro = StringUtils.getFieldFromConcatString(str, "\\|", key)
          StringUtils.isNotEmpty(getPro) && proArr.contains(getPro)
        })
      }
      //过滤访问时间在某个范围内
      case Constants.FIELD_START_TIME => {
        val string = value.asInstanceOf[String]
        if (StringUtils.isNotEmpty(string)) {
          val tuple = string.split("\\|")
          if (tuple.length >= 2) {
            import java.text.SimpleDateFormat
            val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val start = df.parse(tuple(0))
            val end = df.parse(tuple(1))
            if (start.before(end)) {
              filteredRDD = sessionAndUserInfo.filter(str => {
                val getStartTime = StringUtils.getFieldFromConcatString(str, "\\,", key)
                val thisStartTime = df.parse(getStartTime)
                StringUtils.isNotEmpty(getStartTime) && thisStartTime.after(start) && thisStartTime.before(end)
              })
            } else {
              println("输入的时间段不正确!!")
            }
          }
        }
      }
    }
    filteredRDD
  }
}
