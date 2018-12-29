package com.requirement2

import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{SessionRandomExtract, UserVisitAction}
import commons.utils.StringUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

//extends java.io.Serializable 解决序列化问题
class requirement2 extends java.io.Serializable {

  def doRequire2() = {
    val borrowRequire1 = new com.requirement1.Requirement1
    //创建配置对象
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    //此处引用的包为“net.sf.json.JSONObject”
    //将字符串转化为JSON对象，以便于下一步处理
    //此处的JSON对象用于数据初筛选，筛选条件在该JSON字符串中可做修改
    val taskParams = JSONObject.fromObject(jsonStr)

    //生成UUID作为当前任务的ID
    val taskUUID = UUID.randomUUID().toString
    //获取sparkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("radomsession")
    //获取sparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //获取当前初筛选条件下的所有userVisitAction组成的RDD，数据内容如下
    //(日期，用户ID，sessionID，页面ID，动作时间，搜索关键字，点击类别ID，点击产品ID，订单产品ID集合，付款类别ID集合，付款产品ID集合，城市ID)
    val userVisitActionsRDD = borrowRequire1.getActiveRDD(sparkSession, taskParams)
    //UserVisitAction(2018-12-18,68,960ada8a169f41fcbc55b27a9b4a6271,5,2018-12-18 3:30:57,null,-1,-1,89,66,null,null,2)
    //UserVisitAction(2018-12-18,68,960ada8a169f41fcbc55b27a9b4a6271,7,2018-12-18 3:48:36,null,68,92,null,null,null,null,9)
    //UserVisitAction(2018-12-18,68,960ada8a169f41fcbc55b27a9b4a6271,9,2018-12-18 3:11:24,null,6,35,null,null,null,null,9)
    //UserVisitAction(2018-12-18,68,960ada8a169f41fcbc55b27a9b4a6271,6,2018-12-18 3:35:25,null,-1,-1,null,null,27,22,4)

    //将userVisitActionRDD中的每个userVisitAction转换为(userVisitAction.session_id,userVisitAction)的二元组
    val actionsTypeBySession = userVisitActionsRDD.map(userVisitAction => (userVisitAction.session_id, userVisitAction))
    //groupbykey，将actionsTypeBySession转换为斧形数据(k,iteratable)
    val actionsGroupByKey = actionsTypeBySession.groupByKey()
    /*3f4c709731f843b58707758c161cc814,CompactBuffer(
    UserVisitAction(2018-12-18,43,3f4c709731f843b58707758c161cc814,0,2018-12-18 20:08:17,null,-1,-1,88,54,null,null,5),
    UserVisitAction(2018-12-18,43,3f4c709731f843b58707758c161cc814,8,2018-12-18 20:45:00,null,99,87,null,null,null,null,0),
    UserVisitAction(2018-12-18,43,3f4c709731f843b58707758c161cc814,9,2018-12-18 20:14:56,保温杯,-1,-1,null,null,null,null,4),
    UserVisitAction(2018-12-18,43,3f4c709731f843b58707758c161cc814,2,2018-12-18 20:49:05,null,-1,-1,null,null,36,9,0))*/
    //持久化
    actionsGroupByKey.cache()
    val sessionID2FullInfoRDD = borrowRequire1.getFullInfoData(sparkSession, actionsGroupByKey)
    //(08ead1badcfb40b9b161346efd7e7210,sessionid=08ead1badcfb40b9b161346efd7e7210|searchKeywords=苹果小龙虾Lamer联想笔记本保温杯华为手机机器学习吸尘器|clickCategoryIds=13252661688819816579718784454776|visitLength=3382|stepLength=63|startTime=2018-12-20 10:00:12|age=52|professional=professional28|sex=male|city=city66)
    //(3e969c7b943543a1b3cf8501c1d062d6,sessionid=3e969c7b943543a1b3cf8501c1d062d6|searchKeywords=华为手机机器学习Lamer苹果洗面奶|clickCategoryIds=9565218991513|visitLength=3127|stepLength=29|startTime=2018-12-20 16:04:22|age=52|professional=professional28|sex=male|city=city66)
    //(009b713383be446da688468972c5baba,sessionid=009b713383be446da688468972c5baba|searchKeywords=保温杯吸尘器小龙虾苹果机器学习卫生纸Lamer华为手机联想笔记本|clickCategoryIds=747981533480613347813855084862372379490427262|visitLength=3400|stepLength=96|startTime=2018-12-20 19:00:47|age=52|professional=professional28|sex=male|city=city66)

    val actionsTypeByDateHour = getActionsTypeByDateAndHour(sessionID2FullInfoRDD)
    //(2018-12-2011,sessionid=7d25398f12b44381a8c9ec3bb640398b|searchKeywords=机器学习苹果小龙虾联想笔记本Lamer吸尘器保温杯华为手机洗面奶|clickCategoryIds=62954591866727211976|visitLength=3464|stepLength=73|startTime=2018-12-20 11:00:35|age=21|professional=professional31|sex=male|city=city18)
    //(2018-12-2005,sessionid=b7699f48f39246f287e2173f5d845d78|searchKeywords=苹果洗面奶联想笔记本吸尘器卫生纸|clickCategoryIds=284412|visitLength=3410|stepLength=36|startTime=2018-12-20 05:01:14|age=21|professional=professional31|sex=male|city=city18)
    //(2018-12-2005,sessionid=971eeceff8a448118a1ba9220276b09c|searchKeywords=小龙虾吸尘器保温杯苹果联想笔记本华为手机|clickCategoryIds=354414974896895928108720|visitLength=3470|stepLength=58|startTime=2018-12-20 05:00:01|age=21|professional=professional31|sex=male|city=city18)

    val dateHourCountType = getDateHourCount(actionsTypeByDateHour)
    //(2018-12-22,Map(12 -> 45, 15 -> 41, 09 -> 36, 00 -> 49, 21 -> 55, 18 -> 48, 03 -> 40, 06 -> 48, 17 -> 49,
    // 05 -> 43, 11 -> 49, 08 -> 62, 14 -> 49, 20 -> 57, 02 -> 41, 01 -> 43, 22 -> 50, 16 -> 44, 19 -> 61, 04 -> 48,
    // 10 -> 51, 13 -> 59, 07 -> 43))
    val hourRadomSessionList = getHourRadomSessionList(sparkSession, sessionID2FullInfoRDD.count(), 150, dateHourCountType)
    //(2018-12-22,Map(12 -> List(11, 23, 30, 0, 10, 20), 15 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1),
    // 09 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31),
    // 00 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32),
    // 21 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8), 18 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36), 03 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25), 06 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25, 28, 35, 39), 17 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25, 28, 35, 39, 27), 05 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25, 28, 35, 39, 27), 11 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25, 28, 35, 39, 27, 19, 21, 24), 08 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25, 28, 35, 39, 27, 19, 21, 24, 6, 18, 55), 14 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25, 28, 35, 39, 27, 19, 21, 24, 6, 18, 55, 17, 2, 47, 29), 20 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25, 28, 35, 39, 27, 19, 21, 24, 6, 18, 55, 17, 2, 47, 29, 16, 26, 49), 02 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25, 28, 35, 39, 27, 19, 21, 24, 6, 18, 55, 17, 2, 47, 29, 16, 26, 49), 22 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25, 28, 35, 39, 27, 19, 21, 24, 6, 18, 55, 17, 2, 47, 29, 16, 26, 49, 41, 4, 13), 01 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25, 28, 35, 39, 27, 19, 21, 24, 6, 18, 55, 17, 2, 47, 29, 16, 26, 49, 41), 16 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25, 28, 35, 39, 27, 19, 21, 24, 6, 18, 55, 17, 2, 47, 29, 16, 26, 49, 41, 4, 13), 10 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25, 28, 35, 39, 27, 19, 21, 24, 6, 18, 55, 17, 2, 47, 29, 16, 26, 49, 41, 4, 13, 58, 54, 38, 50), 04 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25, 28, 35, 39, 27, 19, 21, 24, 6, 18, 55, 17, 2, 47, 29, 16, 26, 49, 41, 4, 13, 58, 54, 38), 19 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25, 28, 35, 39, 27, 19, 21, 24, 6, 18, 55, 17, 2, 47, 29, 16, 26, 49, 41, 4, 13, 58, 54), 13 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25, 28, 35, 39, 27, 19, 21, 24, 6, 18, 55, 17, 2, 47, 29, 16, 26, 49, 41, 4, 13, 58, 54, 38, 50), 07 -> List(11, 23, 30, 0, 10, 20, 34, 7, 22, 1, 31, 5, 12, 32, 40, 42, 33, 37, 15, 8, 36, 9, 25, 28, 35, 39, 27, 19, 21, 24, 6, 18, 55, 17, 2, 47, 29, 16, 26, 49, 41, 4, 13, 58, 54, 38, 50)))

    getFinalData(sparkSession, taskUUID, hourRadomSessionList, actionsTypeByDateHour)
  }


  /**
    * 将userVisitActionRDD中的每个userVisitAction转换为(userVisitAction.session_id,userVisitAction)的二元组
    * (009b713383be446da688468972c5baba,sessionid=009b713383be446da688468972c5baba|searchKeywords=保温杯吸尘器小龙虾苹果机器学习卫生纸Lamer华为手机联想笔记本|clickCategoryIds=747981533480613347813855084862372379490427262|visitLength=3400|stepLength=96|startTime=2018-12-20 19:00:47|age=52|professional=professional28|sex=male|city=city66)
    *
    **/
  def getActionsTypeByDateAndHour(sessionID2FullInfoRDD: RDD[(String, String)]): RDD[(String, String)] = {
    val dateHour2FullInfoRDD = sessionID2FullInfoRDD.map {
      case (sessionID, fullInfo) =>
        val timeArr = fullInfo.split("\\|") {
          5
        }.split("=") {
          1
        }.split(" ")
        val date = timeArr {
          0
        }
        val hour = timeArr {
          1
        }.split(":") {
          0
        }
        (date + hour, fullInfo)
    }
    dateHour2FullInfoRDD.collect()
    dateHour2FullInfoRDD
  }

  /**
    * 计算每个小时的action数
    * (dateHour,fullInfo) => (date,(hour,count))
    **/
  def getDateHourCount(actionsTypeByDateHour: RDD[(String, String)]) = {
    //得到(dataHour,count)
    //(2018-12-2005,count)
    val dateHour_Count = actionsTypeByDateHour.countByKey()
    val date_hour_count = new mutable.HashMap[String, mutable.HashMap[String, Long]]
    dateHour_Count.map {
      case (dateHour, count) =>
        val date = dateHour.substring(0, 10)
        val hour = dateHour.substring(10, 12)
        date_hour_count.get(date) match {
          case None =>
            date_hour_count(date) = new mutable.HashMap[String, Long]()
            date_hour_count(date) += (hour -> count)
          case Some(map) =>
            date_hour_count(date) += (hour -> count)
        }
    }
    date_hour_count
  }

  /**
    * 获得(date,(hour,list))
    * list里保存的为这个小时里所抽取的session下标号
    *
    * @param sessionCount         当前session总数（筛选后）
    * @param getSessionCountInput 需要抽取的session数
    * @param date_hour_count      一天内每小时的session数
    **/
  def getHourRadomSessionList(sparkSession: SparkSession,
                              sessionCount: Long,
                              getSessionCountInput: Long,
                              date_hour_count: mutable.HashMap[String, mutable.HashMap[String, Long]]) = {
    var getSessionCount: Long = getSessionCountInput
    if (sessionCount < getSessionCountInput) {
      getSessionCount = sessionCount
    }
    var date_hour_list = new mutable.HashMap[String, mutable.HashMap[String, List[Int]]]
    val random = new Random()
    val rss = date_hour_count.map {
      case (date, hourCountMap) =>
        date_hour_list.get(date) match {
          case None =>
            date_hour_list(date) = new mutable.HashMap[String, List[Int]]()
            for (hourCount <- hourCountMap) {
              var hourList: List[Int] = Nil
              val getHourSessionCount = (hourCount._2 * getSessionCount) / sessionCount + 1
              for (i <- 1 to getHourSessionCount.toInt) {
                val r = random.nextInt(hourCount._2.toInt)
                if (!hourList.contains(r)) {
                  hourList = hourList ::: List(r)
                }
              }
              date_hour_list(date) += (hourCount._1 -> hourList)
            }
          case Some(someMap) =>
            for (hourCount <- hourCountMap) {
              var hourList: List[Int] = Nil
              val getHourSessionCount = (hourCount._2 * getSessionCount) / sessionCount
              for (i <- 1 to getHourSessionCount.toInt) {
                val r = random.nextInt(hourCount._2.toInt)
                if (!hourList.exists(s => s == r)) {
                  hourList = hourList ::: List(r)
                }
              }
              date_hour_list(date) += (hourCount._1 -> hourList)
            }
        }
        date_hour_count
    }
    println(date_hour_list)
    date_hour_list
  }


  /**
    * 按照已获得的hourRadomSessionList及数据信息抽取数据并写入数据库
    *
    **/
  def getFinalData(sparkSession: SparkSession,
                   taskUUID: String,
                   hourRadomSessionList: mutable.HashMap[String, mutable.HashMap[String, List[Int]]],
                   actionsTypeByDateHour: RDD[(String, String)]) = {
    val date_hour_listBd = sparkSession.sparkContext.broadcast(hourRadomSessionList)
    println()
    val dateHour2GroupRDD = actionsTypeByDateHour.groupByKey()
    val extractSessionRDD = dateHour2GroupRDD.flatMap {
      case (dateHour, iterableFullInfo) =>
        val date = dateHour.substring(0, 10)
        val hour = dateHour.substring(10, 12)

        val extractList = hourRadomSessionList.get(date).get(hour)

        val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()

        var index = 0
        for (fullInfo <- iterableFullInfo) {
          if (extractList.contains(index)) {
            val sessionid = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
            val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
            val searchKeywords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
            val clickCategoryIds = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
            extractSessionArrayBuffer.append(SessionRandomExtract(taskUUID, sessionid, startTime, searchKeywords, clickCategoryIds))
          }
          index += 1
        }
        extractSessionArrayBuffer
    }
    import sparkSession.implicits._
    extractSessionRDD.toDF().write.format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_random_extract")
      .mode(SaveMode.Append)
      .save()
  }
}
