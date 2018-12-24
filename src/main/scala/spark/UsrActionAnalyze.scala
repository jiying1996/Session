package spark

import java.util
import java.util.{Date, Properties}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.qf.sessionanalyze.constant.Constants
import com.qf.sessionanalyze.dao.factory.DAOFactory
import com.qf.sessionanalyze.domain._
import com.qf.sessionanalyze.test.MockData
import com.qf.sessionanalyze.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object UsrActionAnalyze {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION)
      .setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName(Constants.SPARK_APP_NAME_SESSION).getOrCreate()

    // 生成模拟数据
    MockData.mock(sc, spark)
    // 获取任务
    // 创建访问数据库的实例
    val taskDAO = DAOFactory.getTaskDAO
    // 访问taskDAO对应的数据表
    val taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION)
    //    println(taskId)
    val task = taskDAO.findById(taskId)
    if (task == null) {
      println("没有获取到对应taskID的task信息")
      return
    }
    val taskParam = JSON.parseObject(task.getTaskParam)

    // 获取参数指定范围的数据
    val actionRDD = getActionRDDByDateRange(spark, taskParam)
    // (action)
    // (sessionid, action)

    val sessionId2ActionRDD = actionRDD.rdd.map(row => (row.get(2).toString, row))
    //    sessionId2ActionRDD.take(3).foreach(println)

    // 1.缓存后面要反复使用的RDD
    val sessionId2ActionRDDCache = sessionId2ActionRDD.cache()
    // 2.分区数20，提高并行度？重分区，reparation
    // 3.sc.textFile, sc.parallelize,分区或分片，过程中会发生shuffle？不会

    // 把用户数据和用户访问的行为数据进行一个整合
    val sessionId2AggregateInfoRDD = aggregateByUserid(sc, spark, sessionId2ActionRDD)
    val accum = new SessionAccumulator()
    sc.register(accum, "customAccum")
    //    sessionId2AggregateInfoRDD.take(3).foreach(println)
    //    val scheme = sessionId2AggregateInfoRDD.map(row => Res(row))
    //
    //    val df = spark.createDataFrame(scheme)
    //    val url = "jdbc:mysql://localhost:3306/sessionanalyze?useUnicode=true&characterEncoding=utf8"
    //
    //    val props = new Properties()
    //    props.setProperty("user", "root")
    //    props.setProperty("password", "123456")
    //    val tableName = "sessionBase"
    //    df.write.mode(SaveMode.Append).jdbc(url, table = tableName, props)
    val filteredSessionRDD = filterSessionByParam(sessionId2AggregateInfoRDD, taskParam, accum)
    println("--------------------------------------------------")
    filteredSessionRDD.take(3).foreach(println)
    //        caclculatePercent(accum.value, taskId)
    val sessionDetailRDD = filteredSessionRDD.join(sessionId2ActionRDD).map(tuple => {
      (tuple._1, (tuple._1, tuple._2._2))
    })
    println("--------------------------------------------------")
    sessionDetailRDD.take(3).foreach(println)
    println("我是分界线")

    //  extractSessionByRatio(filteredSessionRDD, sessionDetailRDD, taskId, sc)
    val top10CategoryResult: Array[(String, Int, Int, Int)] = top10CategorySort(sessionDetailRDD, taskId)
    val top10CategoryIds: Array[String] = sc.makeRDD(top10CategoryResult).map(item => {
      item._1 // (品类id, 点击次数)
    }).collect()

    top10CategorySession(sessionDetailRDD, top10CategoryIds, taskId)


  }

  /**
    * top10活跃session (自己实现)
    *
    * @param sessionDetailRDD 明细数据
    * @param top10CategoryIds top10的品类
    * @param taskid           taskId
    */
  def top10CategorySession(sessionDetailRDD: RDD[(String, (String, Row))], top10CategoryIds: Array[String], taskid: Long) = {
    // (sessionid_clickCategoryId, click_count)
    val reduced: RDD[(String, Int)] = sessionDetailRDD.filter(_._2._2.get(6) != null).map(tuple => {
      val sessionid = tuple._1
      val clickCategoryId = tuple._2._2.getLong(6)
      (sessionid + "_" + clickCategoryId, 1)
    }).reduceByKey(_ + _)

    top10CategoryIds.foreach(println)
    // 过滤出需要的 然后再按clickCategoryId分组
    val filtered: RDD[(Long, Iterable[(String, Int)])] = reduced.filter(tuple => {
      val strings: Array[String] = tuple._1.split("_")
      top10CategoryIds.contains(strings(1)) // top10中包含此id，就留下
    }).groupBy(_._1.split("_")(1).toLong) // 按照clickCategoryId分组
    // 每一个都取前十
    val result: RDD[Iterable[(String, Int)]] = filtered.map(item => {
      item._2.toList.sortBy(_._2).reverse.take(10)
    })
    //    val result: RDD[Iterable[(String, Int)]] = filtered.map(_._2.take(10))
    var list = List[(String, String)]()
    result.foreach(ite => {
      val top10SessionDAO = DAOFactory.getTop10SessionDAO
      ite.foreach(item => {
        val top10Session = new Top10Session
        //  类型(sessionid, clickCategoryId, count)
        val sessionid = item._1.split("_")(0)
        val clickCategoryId = item._1.split("_")(1).toLong
        val count = item._2
        top10Session.setTaskid(taskid)
        top10Session.setSessionid(sessionid)
        top10Session.setCategoryid(clickCategoryId)
        top10Session.setClickCount(count)
        top10SessionDAO.insert(top10Session)
      })
    })
  }

  /**
    * 对各品类的top10进行计算(自己实现)
    *
    * @param filterSessionRDD
    * @param sessionDetailRDD
    * @param taskid
    */
  def top10CategorySort(sessionDetailRDD: RDD[(String, (String, Row))], taskid: Long): Array[(String, Int, Int, Int)] = {
    val clickRDD: RDD[(String, Int)] = sessionDetailRDD.filter(_._2._2.get(6) != null).map(tuple => {
      val row = tuple._2._2
      val tup = (row.getLong(6).toString, 1)
      tup
    }).reduceByKey(_ + _)
    val orderRDD = sessionDetailRDD.filter(_._2._2.get(8) != null).map(tuple => {
      val row = tuple._2._2
      val tup = (row.getString(8), 1)
      tup
    }).reduceByKey(_ + _)

    val payRDD: RDD[(String, Int)] = sessionDetailRDD.filter(_._2._2.get(10) != null).map(tuple => {
      val row = tuple._2._2
      val tup = (row.getString(10), 1)
      tup
    }).reduceByKey(_ + _)

    val sorted = clickRDD.leftOuterJoin(orderRDD).leftOuterJoin(payRDD).map(x => {
      (x._1, x._2._1._1, x._2._1._2.getOrElse(0), x._2._2.getOrElse(0))
    }).sortBy(x => Top10CategoryCase(x), false)
    //      .top(10)
    //      .sortBy(x => Top10CategoryCase(x))
    //      .reverse

    val resultList: Array[(String, Int, Int, Int)] = sorted.take(10)
    // 存入数据库中
    val top10CategoryDAO = DAOFactory.getTop10CategoryDAO
    for (item <- resultList) {
      val top10Category = new Top10Category()
      top10Category.setTaskid(taskid)
      top10Category.setCategoryid(item._1.toLong)
      top10Category.setClickCount(item._2)
      top10Category.setOrderCount(item._3)
      top10Category.setPayCount(item._4)

      top10CategoryDAO.insert(top10Category)
    }
    resultList
  }

  /**
    * 按比例随机抽取session
    *
    * @param filterSessionRDD 过滤后的RDD
    * @param sessionDetailRDD session明细信息
    * @param taskid           taskID
    * @param sc               SparkContext
    */
  def extractSessionByRatio(filterSessionRDD: RDD[(String, String)], sessionDetailRDD: RDD[(String, (String, Row))], taskid: Long, sc: SparkContext) = {
    // 1、计算每个小时session个数
    // 最后数据结果格式(date+hour,count)
    val time2SessionIdRDD = filterSessionRDD.map(tuple => {
      // 当前session对应的信息取出来
      val sessionInfo = tuple._2
      // 取出当前会话的开始时间 yyyy-MM-dd hh:mm:ss
      val startTime = StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_START_TIME)
      // 解析出来当前session的日期和小时
      val dateHour = DateUtils.getDateHour(startTime)
      (dateHour, sessionInfo)
    })
    // 调用countByKey算子，计算每天各小时的session个数，返回Map(DateHour, session_count)
    val countMap = time2SessionIdRDD.countByKey()

    // 2、按比例随机抽取
    // 每天应该抽取的session个数
    // 需要转换为格式 (date,(hour,count))
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]
    // 遍历countMap
    //    import scala.collection.JavaConversions._
    countMap.foreach(tuple => {
      val date = tuple._1.split("_")(0)
      val hour = tuple._1.split("_")(1)
      val count = tuple._2
      var hourCountMap = dateHourCountMap.getOrElse(date, null)
      if (hourCountMap == null) {
        hourCountMap = new mutable.HashMap[String, Long]()
        dateHourCountMap.put(date, hourCountMap)

      }
      hourCountMap.put(hour, count)
    })

    // 计算数据集总天数，计算每天抽取的session个数
    val extractNum = 100 / dateHourCountMap.size
    // 计算每天每小时抽取的个数
    val dateHourExtractMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()
    dateHourCountMap.foreach(tuple => {
      //获取日期
      val date = tuple._1
      //当前日期下每小时的session个数
      val hourCountMap = tuple._2
      //计算当天的session总数
      var sessionCount = 0L
      hourCountMap.foreach(tuple => {
        sessionCount += tuple._2
      })
      var hourExtractMap = dateHourExtractMap.getOrElse(date, null)
      if (hourExtractMap == null) {
        hourExtractMap = new mutable.HashMap[String, ListBuffer[Int]]()
        dateHourExtractMap.put(date, hourExtractMap)
      }
      //计算每个小时抽取的session个数
      hourCountMap.foreach(tuple => {
        //取当前小时的session个数
        val hour = tuple._1
        //计算当前小时要抽取的session个数
        val count = tuple._2
        var extractCount = (count.toDouble / sessionCount.toDouble * extractNum).toInt
        // 如果当前小时的总数小于要抽取的个数，就抽取全部
        if (extractCount > count) extractCount = count.toInt
        //计算抽取的session索引信息
        var extractIndexList = new ListBuffer[Int]()
        extractIndexList = hourExtractMap.getOrElse(hour, null)
        if (extractIndexList == null) {
          extractIndexList = new ListBuffer[Int]()
          hourExtractMap.put(hour, extractIndexList)
        }
        val random = new Random()
        //随机生成要抽取的数据的索引
        var i = 0
        while (i < extractCount) {
          var extractIndex = random.nextInt(count.toInt)
          //判断随机的索引是否重复
          while (extractIndexList.contains(extractIndex)) {
            extractIndex = random.nextInt(count.toInt)
          }
          extractIndexList.append(extractIndex)
          i += 1
        }
      })
    })

    // 把抽取的信息(date,(hour,(1,34,44)))广播到每一个executor
    val dateHourExtractMapBroadcast = sc.broadcast(dateHourExtractMap)
    // 3.根据生成的dateHourExtractMap字典，在task上抽取相应的session
    // (dateHour, sessionInfo) => (dateHour, iterator(sessionInfo)
    val time2SessionRDD = time2SessionIdRDD.groupByKey()
    // 根据计算出来的索引信息抽取具体的session
    val extractSessionRDD = time2SessionRDD.map(
      tuple => {
        // 存储我们抽取的结果 (sessionId, sessionId)
        val extractSessionIds = new util.ArrayList[(String, String)]()
        val dateHour = tuple._1
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)
        // 获取广播变量
        val dateHourExtractMap = dateHourExtractMapBroadcast.value
        // 获取抽取的索引列表
        val extractIndexList = dateHourExtractMap.get(date).get(hour)
        val sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO
        val it = tuple._2.iterator
        var index = 0
        var sessionid = ""
        while (it.hasNext) {
          val sessionInfo = it.next()
          // 当前的索引是否在抽取的索引列表里
          if (extractIndexList.contains(index)) {
            sessionid = StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_SESSION_ID)
            val sessionRandomExtract = new SessionRandomExtract
            sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_START_TIME))
            sessionRandomExtract.setSessionid(sessionid)
            sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS))
            sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS))
            // 插入数据库中
            sessionRandomExtractDAO.insert(sessionRandomExtract)
            // 将我们抽取的sessionId存储到list中
            extractSessionIds.add((sessionid, sessionid))
          }
          index += 1
        }
        (sessionid, sessionid)
      }
    )

    // 4.获取抽取的session的明细数据(sessionid,(sessionid,(sessionid,row)))
    val extractSessionDetailRDD = extractSessionRDD.join(sessionDetailRDD)
    val sessionDetails = new util.ArrayList[SessionDetail]()

    extractSessionDetailRDD.foreachPartition(partition => {
      partition.foreach(tuple => {
        val row = tuple._2._2._2
        val sessionDetail = new SessionDetail()
        sessionDetail.setTaskid(taskid)
        sessionDetail.setUserid(row.getLong(1))
        sessionDetail.setSessionid(row.getString(2))
        sessionDetail.setPageid(row.getLong(3))
        sessionDetail.setActionTime(row.getString(4))
        sessionDetail.setSearchKeyword(row.getString(5))
        sessionDetail.setClickCategoryId(row.getAs[Long](6))
        sessionDetail.setClickProductId(row.getAs[Long](7))
        sessionDetail.setOrderCategoryIds(row.getString(8))
        sessionDetail.setOrderProductIds(row.getString(9))
        sessionDetail.setPayCategoryIds(row.getString(10))
        sessionDetail.setPayProductIds(row.getString(11))
        // TODO
        sessionDetails.add(sessionDetail)

      })
      val sessionDetailDAO = DAOFactory.getSessionDetailDAO
      sessionDetailDAO.insertBatch(sessionDetails)
    })
  }

  /**
    * 过滤session
    *
    * @param sessionId2AggregateInfoRDD
    * @param taskParam
    * @param sessionAccumulator
    */
  def filterSessionByParam(sessionId2AggregateInfoRDD: RDD[(String, String)], taskParam: JSONObject, sessionAccumulator: SessionAccumulator): RDD[(String, String)] = {
    // 解析task的参数信息
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionInfo = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val city = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categorys = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    // 参数拼接成一个字符串
    var param = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionInfo != null) Constants.PARAM_PROFESSIONALS + "=" + professionInfo + "|" else "") +
      (if (city != null) Constants.PARAM_CITIES + "=" + city + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categorys != null) Constants.PARAM_CATEGORY_IDS + "=" + categorys + "|" else "")
    // 如果param结尾是‘|’，截掉
    if (param.endsWith("|")) param = param.substring(0, param.length - 1)

    // 把拼接之后的参数，传给过滤函数，根据参数值对数据集进行过滤
    // 把每一条数据过滤，如果满足参数条件的，数据中访问时、访问步长取出来，通过累加器把相应的字段值累加
    sessionId2AggregateInfoRDD.filter(filterFun(_, param, sessionAccumulator))
  }

  /**
    * 按照条件过滤，并计算出各个时长和步长
    *
    * @param tuple
    * @param parame
    * @param accumulator
    * @return
    */
  def filterFun(tuple: (String, String), parame: String, accumulator: SessionAccumulator): Boolean = {
    // session信息取出来
    val info = tuple._2
    // 比较info里面信息是否满足param
    if (!ValidUtils.between(info, Constants.FIELD_AGE, parame, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
      return false
    }
    if (!ValidUtils.in(info, Constants.FIELD_PROFESSIONAL, parame, Constants.PARAM_PROFESSIONALS)) {
      return false
    }
    if (!ValidUtils.in(info, Constants.FIELD_CITY, parame, Constants.PARAM_CITIES)) {
      return false
    }
    if (!ValidUtils.in(info, Constants.FIELD_SEX, parame, Constants.PARAM_SEX)) {
      return false
    }
    if (!ValidUtils.in(info, Constants.FIELD_SEARCH_KEYWORDS, parame, Constants.PARAM_KEYWORDS)) {
      return false
    }
    if (!ValidUtils.in(info, Constants.FIELD_CLICK_CATEGORY_IDS, parame, Constants.PARAM_CATEGORY_IDS)) {
      return false
    }

    // 把满足条件的这条session的访问时长和访问步长取出，使用累加器进行累加
    // 累加总的session_count
    accumulator.add(Constants.SESSION_COUNT)
    // 先把该session的访问时长和访问步长取出来
    val visitLength = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_VISIT_LENGTH).toLong / 1000
    val stepLength = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_STEP_LENGTH).toLong
    // 判断当前访问时长和访问步长的分布区间，然后累加对应的值

    SessionAggregate.calculateVisitLength(accumulator, visitLength)
    SessionAggregate.calculateStepLength(accumulator, stepLength)

    true
  }

  /**
    * 计算各个范围访问时长、访问步长在总的session中占比
    *
    * @param accumulatorValue
    * @param taskid
    */
  def caclculatePercent(accumulatorValue: String, taskid: Long) = {
    // 计算各个访问时长和访问步长占比
    val session_count = StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.SESSION_COUNT).toLong
    val visit_length_1s_3s = StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_1s_3s)
    val visit_length_4s_6s = StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_4s_6s)
    val visit_length_7s_9s = StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_7s_9s)
    val visit_length_10s_30s = StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_10s_30s)
    val visit_length_30s_60s = StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_30s_60s)
    val visit_length_1m_3m = StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_1m_3m)
    val visit_length_3m_10m = StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_3m_10m)
    val visit_length_10m_30m = StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_10m_30m)
    val visit_length_30m = StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_30m)

    //
    val step_length_1_3 = StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.STEP_PERIOD_1_3)
    val step_length_4_6 = StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.STEP_PERIOD_4_6)
    val step_length_7_9 = StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.STEP_PERIOD_7_9)
    val step_length_10_30 = StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.STEP_PERIOD_10_30)
    val step_length_30_60 = StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.STEP_PERIOD_30_60)
    val step_length_60 = StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.STEP_PERIOD_60)

    //
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s.toDouble / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s.toDouble / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s.toDouble / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s.toDouble / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s.toDouble / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m.toDouble / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m.toDouble / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m.toDouble / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m.toDouble / session_count, 2)
    // 
    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3.toDouble / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6.toDouble / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9.toDouble / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30.toDouble / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60.toDouble / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60.toDouble / session_count, 2)

    //
    val sessionAggrStat = new SessionAggrStat
    sessionAggrStat.setTaskid(taskid)
    sessionAggrStat.setSession_count(session_count)
    sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio)
    sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio)
    sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio)
    sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio)
    sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio)
    sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio)
    sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio)
    sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio)
    sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio)

    sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio)
    sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio)
    sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio)
    sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio)
    sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio)
    sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio)
    //

    val sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO
    sessionAggrStatDAO.insert(sessionAggrStat)
  }

  /**
    * 合并用户信息和session信息
    *
    * @param sc
    * @param spark
    * @param sessionId2ActionRDD
    * @return
    */
  def aggregateByUserid(sc: SparkContext, spark: SparkSession, sessionId2ActionRDD: RDD[(String, Row)]): RDD[(String, String)] = {
    // 先把数据根据sessionid进行聚合
    val sessionIdGroupBy = sessionId2ActionRDD.groupByKey()
    sessionIdGroupBy.take(3).foreach(println)
    // 把当前sessionID下的所有行为进行聚合
    val sessionuserinfo = sessionIdGroupBy.map(tuple => {
      val sessionid = tuple._1
      val searchKeyWordBuffer = new StringBuffer()
      val clickCategoryIdsBuffer = new StringBuffer()
      // 用户的id信息
      var usrid = 0L
      var startTime = new Date()
      var endTime = new Date()
      // 当前会话的访问步长
      var stepLength = 0
      for (elem <- tuple._2.iterator) {
        usrid = elem.getLong(1)
        val searchekeyword = elem.getString(5)
        val clickCategoryid = String.valueOf(elem.getAs[Long](6))
        // 搜索词和点击的品类信息追加到汇总的stringBuffer中
        if (!StringUtils.isEmpty(searchekeyword)) {
          if (!searchKeyWordBuffer.toString.contains(searchekeyword)) {
            searchKeyWordBuffer.append(searchekeyword + ",")
          }
        }
        if (clickCategoryid != null) {
          if (!clickCategoryIdsBuffer.toString.contains(clickCategoryid)) {
            clickCategoryIdsBuffer.append(clickCategoryid + ",")
          }
        }
        // 计算session的开始时间和结束时间
        val actionTime = DateUtils.parseTime(elem.getString(4))
        if (actionTime.before(startTime))
          startTime = actionTime
        if (actionTime.after(endTime))
          endTime = actionTime

        stepLength += 1
      }

      // 截取搜索关键字和点击品类的字符串的','
      val searchWords = StringUtils.trimComma(searchKeyWordBuffer.toString)
      val clickCategorys = StringUtils.trimComma(clickCategoryIdsBuffer.toString)

      // 计算当前会话的访问时长
      val visitLength = endTime.getTime - startTime.getTime

      // 聚合数据，key=value|key=value
      val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|" + Constants.FIELD_SEARCH_KEYWORDS +
        "=" + searchWords + "|" + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategorys + "|" +
        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
        Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) + "|"

      (usrid, aggrInfo)
    })

    // 查询用户的信息
    val sql = "select * from user_info"
    val userinfoRDD = spark.sql(sql).rdd
    val userinfoRDD2 = userinfoRDD.map(row => (row.getLong(0), row))
    val userFullInfo: RDD[(Long, (String, Row))] = sessionuserinfo.join(userinfoRDD2)
    // 处理成(sessionid, session行为信息+用户信息)
    val userSessionFullInfo = userFullInfo.map(tuple => {
      // 整个会话的信息
      val sessioninfo = tuple._2._1
      // 用户信息
      val userinfo = tuple._2._2
      // 取出用户的sessionid
      val sessionid = StringUtils.getFieldFromConcatString(sessioninfo, "\\|", Constants.FIELD_SESSION_ID)
      // 取用户的所有信息
      val age = userinfo.getAs[Int]("age")
      val professional = userinfo.getAs[String]("professional")
      val city = userinfo.getAs[String]("city")
      val sex = userinfo.getAs[String]("sex")
      val resultStr = sessioninfo + Constants.FIELD_AGE + "=" + age + "|" +
        Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
        Constants.FIELD_CITY + "=" + city + "|" + Constants.FIELD_SEX + "=" + sex + "|"

      (sessionid, resultStr)
    })
    userSessionFullInfo
  }

  def getActionRDDByDateRange(spark: SparkSession, taskParam: JSONObject): DataFrame = {
    // 解析参数 拿到开始日期，结束日期
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    // 写一个sql，过滤满足条件的数据
    val sqlStr = "select * from user_visit_action where date >= '" + startDate +
      "' and date <= '" + endDate + "' "
    val actionDF = spark.sql(sqlStr)

    // 展示前五行数据
    println("count:" + actionDF.count())
    actionDF.show(5)

    actionDF
  }
}

case class Res(line: String)

case class Top10CategoryCase(tup: (String, Int, Int, Int)) extends Ordered[Top10CategoryCase] with Serializable {
  override def compare(that: Top10CategoryCase): Int = {
    if (this.tup._2 != that.tup._2) {
      this.tup._2 - that.tup._2 //首先按照点击品类升序
    } else if (this.tup._3 != that.tup._3) {
      this.tup._3 - that.tup._3 // 再按照订单品类升序
    } else
      this.tup._4 - that.tup._4 // 最后按照支付品类升序
  }
}