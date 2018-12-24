package spark

import java.util
import java.util.{Date, Properties}

import com.qf.sessionanalyze.dao.factory.DAOFactory
import com.qf.sessionanalyze.domain._
import com.qf.sessionanalyze.util.DateUtils
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

object AdClickRealTime {
  def main(args: Array[String]): Unit = {
    val processingInterval = 2
    val brokers = "mini1:9092,mini2:9092,mini3:9092"
    val topics = "test1"
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamingWithCheckpoint").setMaster("local[2]")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val checkpointPath = "E://test/sessionAnCP-03"

    //    def functionToCreateContext(): StreamingContext = {
    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    ssc.checkpoint(checkpointPath)
    messages.checkpoint(Duration(8 * processingInterval.toInt * 1000))
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 过滤并更新黑名单
    val filtered = filterAndAdd2BlackList(sparkSession, messages)
    // (time, province, city, userid, adid)

    // 3.实现每天各省各城市各广告的点击流量统计实时统计
    getProvinceCityAdClick(filtered)

    // 4.统计每天各省top3热门广告
    getProvinceTop3(filtered)
    //    getProvinceTop3BySql(sparkSession) // 由于是要实时统计，所以不适用

    // 5.统计各广告最近1小时内的点击量趋势，各广告最近1小时内各分钟的点击量，也是基于第2点的数据基础之上
    getLastHourEachMinut(filtered)

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 5.统计各广告最近1小时内的点击量趋势，各广告最近1小时内各分钟的点击量，也是基于第2点的数据基础之上
    *
    * @param filtered 第二步过滤后的结果 (time, province, city, userid, adid)
    */
  def getLastHourEachMinut(filtered: DStream[(String, String, String, Long, Long)]) = {
    val reduced: DStream[(String, Int)] = filtered.map(tuple => {
      val times = tuple._1.split(" ")
      val day = times(0)
      val hour = times(1).split(":")(0)
      val minute = times(1).split(":")(1)
      val adId = tuple._5
      (day + "_" + hour + "_" + minute + "_" + adId, 1)
    })
      // 按照key进行窗口的聚合 窗口大小为1小时，滑动间隔为一分钟
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Minutes(60), Minutes(1))

    reduced.foreachRDD(rdd =>
      rdd.foreachPartition(it => {
        val list = new util.ArrayList[AdClickTrend]()
        //        while (it.hasNext) {
        it.foreach(tuple => {
          //          val tuple = it.next()
          val items = tuple._1.split("_")
          val day = items(0)
          val hour = items(1)
          val minute = items(2)
          val adId = items(3).toInt
          val count = tuple._2
          val adClickTrend = new AdClickTrend
          adClickTrend.setDate(day)
          adClickTrend.setHour(hour)
          adClickTrend.setMinute(minute)
          adClickTrend.setAdid(adId)
          adClickTrend.setClickCount(count)
          list.add(adClickTrend)
        })
        if (list != null && list.size() > 0) {
          var adClickTrendDAO = DAOFactory.getAdClickTrendDAO
          while (adClickTrendDAO == null)
            adClickTrendDAO = DAOFactory.getAdClickTrendDAO
          adClickTrendDAO.updateBatch(list)
        }
      }))
  }

  /**
    * 4.统计每天各省top3热门广告
    *
    * @param filtered 过滤后的结果 (time, province, city, userid, adid)
    */
  def getProvinceTop3BySql(sparkSession: SparkSession) = {
    val url = "jdbc:mysql://localhost:3306/sessionanalyze?useUnicode=true&characterEncoding=utf8"
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    val tableName = "ad_stat"
    sparkSession.read.jdbc(url, tableName, props).createOrReplaceTempView("ad_stat")
    val sql =
      """
        |select date, province, ad_id, click_count from (
        |select date, province, ad_id ,click_count, 
        |row_number() over(partition by date, province order by click_count desc) as rnk
        |from (select date, province, ad_id, click_count from ad_stat
        |group by date, province, ad_id, click_count) as temp) where rnk <= 3
      """.stripMargin
    sparkSession.sql(sql).rdd.foreachPartition(it => {
      val adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO
      val list = new util.ArrayList[AdProvinceTop3]()
      while (it.hasNext) {
        val row = it.next()
        val date = row.getString(0)
        val province = row.getString(1)
        val ad_id = row.getInt(2)
        val click_count = row.getInt(3)
        val adProvinceTop = new AdProvinceTop3
        adProvinceTop.setProvince(province)
        adProvinceTop.setDate(date)
        adProvinceTop.setAdid(ad_id)
        adProvinceTop.setClickCount(click_count)
        list.add(adProvinceTop)
      }
      adProvinceTop3DAO.updateBatch(list)
    })
  }

  def getProvinceTop3(filtered: DStream[(String, String, String, Long, Long)]) = {
    filtered.map(tuple => {
      // 拿到精确的天的日期
      val date = tuple._1.split(" ")(0)
      (date + "_" + tuple._2 + "_" + tuple._5, 1)
    }).updateStateByKey(func, new HashPartitioner(2), true)
      .foreachRDD(rdd => {
        val grouped: RDD[(String, Iterable[Int])] = rdd.groupByKey()
        grouped.foreachPartition(it => {
          val list = new util.ArrayList[AdProvinceTop3]()
          //          while (it.hasNext) {
          it.foreach(tup => {
            //            val tup = it.next()
            val key = tup._1
            val values: List[Int] = tup._2.toList.sortBy(-_).take(3)
            for (item <- values) {
              val strings = key.split("_")
              val date = strings(0)
              val province = strings(1)
              val adId = strings(2)
              val count = item
              val adProvinceTop3 = new AdProvinceTop3
              adProvinceTop3.setDate(date)
              adProvinceTop3.setProvince(province)
              adProvinceTop3.setAdid(adId.toLong)
              adProvinceTop3.setClickCount(count)
              list.add(adProvinceTop3)
            }
          })
          if (list != null && list.size() > 0) {
            var adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO
            while (adProvinceTop3DAO == null)
              adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO
            adProvinceTop3DAO.updateBatch(list)
          }
        })
      })
  }

  /**
    * 3.实现每天各省各城市各广告的点击流量统计实时统计
    *
    * @param filtered 过滤后的结果 (time, province, city, userid, adid)
    */
  def getProvinceCityAdClick(filtered: DStream[(String, String, String, Long, Long)]) = {
    filtered.map(tuple => {
      // 拿到精确到天的日期
      val date = tuple._1.split(" ")(0)
      (date + "_" + tuple._2 + "_" + tuple._3 + "_" + tuple._5, 1)
    }).updateStateByKey(func, new HashPartitioner(2), true)
      .foreachRDD(rdd => {
        rdd.foreachPartition(it => {
          val list = new util.ArrayList[AdStat]()
          //          while (it.hasNext) {
          it.foreach(tup => {
            //            val tup = it.next()
            val x = tup._1
            val count = tup._2
            // 更新adStat表

            val info = x.split("_")
            val date = info(0)
            val province = info(1)
            val city = info(2)
            val adId = info(3).toLong

            val adStat = new AdStat
            adStat.setDate(date)
            adStat.setProvince(province)
            adStat.setCity(city)
            adStat.setAdid(adId)
            adStat.setClickCount(count)
            list.add(adStat)
          })
          if (list != null && list.size() > 0) {
            var adStatDAO = DAOFactory.getAdStatDAO
            while (adStatDAO == null)
              adStatDAO = DAOFactory.getAdStatDAO
            adStatDAO.updateBatch(list)
          }
        })
      })
  }

  /**
    * 过滤并更新黑名单
    *
    * @param sparkSession SparkSession对象
    * @param messages     最先拿到的DStream
    * @return 返回过滤后的数据 (time, province, city, userid, adid)
    */
  def filterAndAdd2BlackList(sparkSession: SparkSession, messages: InputDStream[(String, String)]) = {
    //1.黑名单
    val url = "jdbc:mysql://localhost:3306/sessionanalyze?useUnicode=true&characterEncoding=utf8"
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    val tableName = "ad_blacklist"

    var blackList: List[Int] = sparkSession.read.jdbc(url, tableName, props).rdd.map(_.getInt(0)).collect().toList.distinct
    // 生成个字段的tuple
    val tups = messages.filter(_._2.split(" ").length >= 5).map(line => {
      val lineArray = line._2.split(" ")
      // 数据格式为：timestamp province city userId adId
      val timeStamp = lineArray(0).toLong
      val date = DateUtils.formatDate(new Date(timeStamp))
      val userId = lineArray(3).toLong
      val adId = lineArray(4).toLong
      (date, userId, adId, 1)
    })

    // 检索并过滤掉黑名单里的数据
    val filtered = tups.filter(x => !blackList.contains(x._2)).map(tuple => {
      (tuple._1 + "_" + tuple._2 + "_" + tuple._3, 1)
    })

    // 将新次生成的进行过滤判断是否是垃圾数据 再过滤掉计算之后点击量大于100的，并把它加到mysql中
    filtered.updateStateByKey(func, new HashPartitioner(2), true)
      .foreachRDD(rdd => {
        rdd.foreachPartition(it => {
          var adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO
          var adBlacklistDAO = DAOFactory.getAdBlacklistDAO
          val adUserClickCounts = new util.ArrayList[AdUserClickCount]()
          val adBlacklists = new util.ArrayList[AdBlacklist]()
          it.foreach(tup => {
            val selectAdBlacklists = adBlacklistDAO.findAll()
            // val tup = it.next()
            val str = tup._1
            val count = tup._2

            val adUserClickCount = new AdUserClickCount
            val strings = str.split("_")
            // date_userId_adId, count
            adUserClickCount.setDate(strings(0))
            adUserClickCount.setUserid(strings(1).toLong)
            adUserClickCount.setAdid(strings(2).toInt)
            adUserClickCount.setClickCount(count)
            // 更新ad_user_click_count表
            val getCount = adUserClickCountDAO.findClickCountByMultiKey(strings(0), strings(1).toLong, strings(2).toInt)
            if (count != getCount) {
              adUserClickCounts.add(adUserClickCount)
            }
            if (getCount >= 100 || count >= 100) {
              // 更新黑名单表
              val userId = str.split("_")(1).toLong
              val adBlacklist = new AdBlacklist

              adBlacklist.setUserid(userId)
              adBlacklists.add(adBlacklist)
              val iter = selectAdBlacklists.listIterator()
              while (iter.hasNext) {
                val selectAdBlacklist = iter.next()
                if (selectAdBlacklist.getUserid == userId) {
                  adBlacklists.remove(adBlacklist)
                }
              }
            }
          })

          if (adUserClickCounts != null) {
            while (adUserClickCountDAO == null) {
              adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO
            }
            adUserClickCountDAO.updateBatch(adUserClickCounts)
          }
          if (adBlacklists != null) {
            while (adBlacklistDAO == null) {
              adBlacklistDAO = DAOFactory.getAdBlacklistDAO
            }
            adBlacklistDAO.insertBatch(adBlacklists)
          }
        })
      })

    messages.filter(_._2.split(" ").length >= 5).map(line => {
      val lineArray = line._2.split(" ")
      // 数据格式为：timestamp province city userId adId
      val timeStamp = lineArray(0).toLong
      val date: String = DateUtils.formatTime(new Date(timeStamp.toLong))
      val province = lineArray(1)
      val city = lineArray(2)
      val userId = lineArray(3).toLong
      val adId = lineArray(4).toLong
      (date, province, city, userId, adId)
    })
      // 再次进行过滤得到最新的不在黑名单里的数据
      // (time, province, city, userid, adid)
      .filter(x => !blackList.contains(x._4))
  }

  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map {
      case (x, y, z) => {
        (x, y.sum + z.getOrElse(0))
      }
    }
  }
}