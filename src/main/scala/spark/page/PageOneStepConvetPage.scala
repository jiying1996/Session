package spark.page

import com.alibaba.fastjson.{JSON, JSONObject}
import com.qf.sessionanalyze.constant.Constants
import com.qf.sessionanalyze.dao.factory.DAOFactory
import com.qf.sessionanalyze.domain.PageSplitConvertRate
import com.qf.sessionanalyze.test.MockData
import com.qf.sessionanalyze.util.{DateUtils, NumberUtils, ParamUtils}
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import spark.usrActionAnalyze2.getActionRDDByDateRange

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object PageOneStepConvetPage {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().appName(Constants.SPARK_APP_NAME_SESSION).getOrCreate()

    //生成模拟数据
    MockData.mock(sc, sparkSession)
    //获取任务
    //创建访问数据库的实例
    val taskDAO = DAOFactory.getTaskDAO
    //访问taskDAO对应的数据表
    val taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE)
    println(taskId)
    val task = taskDAO.findById(taskId)
    if (task == null) {
      println("没有获取到对应taskID的task信息")
      return
    }

    val taskParam = JSON.parseObject(task.getTaskParam)
    // 获取参数指定范围的数据
    val actionRDD = getActionRDDByDateRange(sparkSession, taskParam).rdd
    // (session, row)
    val sessionId2ActionRDD = actionRDD.map(row => {
      (row.getString(2), row)
    })
    // 按sessionId进行分组，拿到一次会话的所有行为
    val groupedSessionId2ActionRDD = sessionId2ActionRDD.groupByKey()
    // 计算每个session用户的访问轨迹 1->2->3 =>页面切片
    val pageSplitRDD = generatePageSplit(sc, groupedSessionId2ActionRDD, taskParam)
    // ("1_2", count)
    val pageSplitMap = pageSplitRDD.countByKey()

    // 统计开始页面的访问次数
    val startPageVisitCount = getStartPageVisit(groupedSessionId2ActionRDD, taskParam)
    // 计算页面的单跳转化率
    val convertRateMap = computePageSplitConvertRate(taskParam, pageSplitMap, startPageVisitCount)

    // 把上一步的结果保存到数据库(taskId, "1_2=0.88|2_3|0.55...")
    insertConvertRateToDB(taskId, convertRateMap)
  }

  def insertConvertRateToDB(taskid: Long, convertRateMap: mutable.HashMap[String, Double]) = {
    // 把convertMap中的数据转化位字符串 (taskId, "1_2=0.88|2_3|0.55...")
    val convertRate = new StringBuilder()
    //    for (elem <- convertRateMap) {
    //      convertRate.append(elem._1 + "=" + elem._2 + "|")
    //    }
    for (i <- 1 to convertRateMap.size) {
      convertRate.append(i + "_" + (i + 1) + "=" + convertRateMap.getOrElse(i + "_" + (i + 1), 0) + "|")
    }
    val res = convertRate.toString().substring(0, convertRate.length - 1)
    val pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO
    val pageSplitConvertRate = new PageSplitConvertRate
    pageSplitConvertRate.setTaskid(taskid)
    pageSplitConvertRate.setConvertRate(res)
    pageSplitConvertRateDAO.insert(pageSplitConvertRate)
  }

  /**
    * 计算单跳转换率
    *
    * @param taskParam
    * @param pageSplitMap
    * @param startPageVisitCount
    * @return
    */
  def computePageSplitConvertRate(taskParam: JSONObject, pageSplitMap: collection.Map[String, Long], startPageVisitCount: Long) = {
    // 解析参数，访问目标页面流
    val targetPages = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",")
    // 上个切片的pv
    var lastPageSplitPV = 0.0
    // 根据要计算的切片的访问率，计算每一个切片的转化率（单跳转化率）
    val convertRateMap = new mutable.HashMap[String, Double]()
    var i = 1
    while (i < targetPages.length) {
      val targetPageSplit = targetPages(i - 1) + "_" + targetPages(i)
      val targetPageSplitPV = pageSplitMap.getOrElse(targetPageSplit, 0.0).toString.toDouble
      var convertRate = 0.0
      if (startPageVisitCount != 0) {
        if (i == 1) {
          convertRate = NumberUtils.formatDouble(targetPageSplitPV / startPageVisitCount.toDouble, 2)
        } else
          convertRate = NumberUtils.formatDouble(targetPageSplitPV / lastPageSplitPV, 2)
        convertRateMap.put(targetPageSplit, convertRate)
        lastPageSplitPV = targetPageSplitPV
      }
      println(targetPages(i - 1) + "的数量为" + targetPageSplitPV)
      i += 1
    }
    convertRateMap
  }

  //获取开始页面的访问量
  def getStartPageVisit(groupedSessionId2ActionRDD: RDD[(String, Iterable[Row])], taskParam: JSONObject) = {
    // 解析首页是哪个页面
    val targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    val startPageId = targetPageFlow.split(",")(0).toLong
    println(startPageId)
    // 定义一个容器，存放到达第一个页面的访问
    val list = new mutable.ListBuffer[Long]()
    val startPageRDD = groupedSessionId2ActionRDD.map(tup => {
      val it = tup._2.iterator
      while (it.hasNext) {
        val row = it.next()
        val pageId = row.getAs[Long]("page_id")
        if (pageId == startPageId){
          list.append(pageId) 
        }
      }
      list
    })
    println("开始页面的访问量：" + startPageRDD.count())
    startPageRDD.count()
  }

  /**
    * 求出所有的切片
    *
    * @param sc
    * @param groupedSessionId2ActionRDD
    * @param taskParam
    * @return
    */
  def generatePageSplit(sc: SparkContext, groupedSessionId2ActionRDD: RDD[(String, Iterable[Row])], taskParam: JSONObject) = {
    // 解析参数，拿到目标页面流
    val targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    val targetPageFlowBroadCast = sc.broadcast(targetPageFlow)
    // ("1_2", 2)
    // 用户所有的切片信息
    val list = new ListBuffer[(String, Integer)]()
    // 计算每一个session中复合条件的页面切片
    // (sessionid, iterator(action))
    groupedSessionId2ActionRDD.flatMap(tup => {
      val it = tup._2.iterator
      // 取目标页面流
      val targetPages = targetPageFlowBroadCast.value.split(",")
      // 遍历当前会话的页面流
      val rows = new mutable.ListBuffer[Row]
      while (it.hasNext) {
        rows += it.next()
      }
      // 按照时间，把当前会话的所有行为进行排序
      implicit val keyOrder = new Ordering[Row] {
        override def compare(x: Row, y: Row): Int = {
          // "yyyy-MM-dd hh:mm:ss"
          val actionTime1 = x.getString(4)
          val actionTime2 = y.getString(4)
          val dateTime1 = DateUtils.parseTime(actionTime1)
          val dateTime2 = DateUtils.parseTime(actionTime2)
          if (dateTime1.getTime >= dateTime2.getTime) {
            1
          } else
            -1
        }
      }
      rows.sorted
      // 生成页面切片
      import scala.util.control.Breaks._
      var lastPageId = -1L
      var tempList = new ListBuffer[String]()
      for (row <- rows) {
        val pageId = row.getLong(3)
        breakable {
          if (lastPageId == -1L) {
            lastPageId = pageId
            break() // 类似于java中的continue
          }
          val pageSplit = lastPageId + "_" + pageId
          // 判断当前切片在不在目标页面流
          var i = 1
          breakable {
            while (i < targetPages.length) {
              val targetPageSplit = targetPages(i - 1) + "_" + targetPages(i)
              if (StringUtils.equals(pageSplit, targetPageSplit)) {
                list.append((pageSplit, 1))
                break()
              }
              i += 1
            }
          }
          lastPageId = pageId
        }
      }
      list
    })
  }
}
