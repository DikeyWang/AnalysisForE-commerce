package com.requirement3

import java.util.UUID

import com.CaseClass.SortKey
import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{Top10Category, Top10Session, UserVisitAction}
import commons.utils.{ParamUtils, StringUtils, ValidUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

class Requirement3 extends java.io.Serializable {

  def doRequireMent3() = {
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
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("top10Category")
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

    val filterData = getFilterData(taskParams, sessionID2FullInfoRDD)
    //过滤后的（sessiid,fullInfo)
    //(44dc1c2cc11b4e119ad9ec54477825cc,sessionid=44dc1c2cc11b4e119ad9ec54477825cc|searchKeywords=Lamer苹果洗面奶机器学习小龙虾华为手机吸尘器卫生纸联想笔记本保温杯|clickCategoryIds=578939426747214197519154514214976918|visitLength=3421|stepLength=79|startTime=2018-12-22 03:00:13|age=32|professional=professional66|sex=male|city=city94)
    //(fa3ab53864d34be7af7366d3ded3c53f,sessionid=fa3ab53864d34be7af7366d3ded3c53f|searchKeywords=联想笔记本Lamer机器学习吸尘器洗面奶苹果华为手机保温杯卫生纸小龙虾|clickCategoryIds=195534277876789147137123878793835132127|visitLength=3379|stepLength=99|startTime=2018-12-22 11:01:57|age=32|professional=professional66|sex=male|city=city94)
    //(8e861e8f0f734faca3343f339e668b15,sessionid=8e861e8f0f734faca3343f339e668b15|searchKeywords=保温杯|clickCategoryIds=84|visitLength=2707|stepLength=11|startTime=2018-12-22 05:09:02|age=32|professional=professional66|sex=male|city=city94)

    //获取能够通过过滤的session的action
    val sessionid2FilterActionRDD = actionsTypeBySession.join(filterData).map {
      case (sessionID, (action, fullInfo)) =>
        (sessionID, action)
    }
    // UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,2,2018-12-22 8:35:58,null,-1,-1,null,null,23,12,9),
    // UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,9,2018-12-22 8:00:57,null,-1,-1,null,null,3,8,2),
    // UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,0,2018-12-22 8:18:27,null,25,40,null,null,null,null,7), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,4,2018-12-22 8:26:30,null,63,39,null,null,null,null,4),
    // UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,0,2018-12-22 8:43:31,小龙虾,-1,-1,null,null,null,null,7), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,1,2018-12-22 8:43:28,null,-1,-1,null,null,8,53,1), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,5,2018-12-22 8:32:02,null,-1,-1,null,null,40,92,7), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,6,2018-12-22 8:00:55,null,91,20,null,null,null,null,8), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,6,2018-12-22 8:53:50,null,20,56,null,null,null,null,7), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,5,2018-12-22 8:52:50,null,88,50,null,null,null,null,7), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,9,2018-12-22 8:50:52,null,-1,-1,60,50,null,null,0), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,5,2018-12-22 8:46:47,null,-1,-1,null,null,72,44,6), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,9,2018-12-22 8:44:36,null,-1,-1,null,null,73,54,9), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,2,2018-12-22 8:29:07,null,-1,-1,null,null,96,79,4), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,9,2018-12-22 8:23:53,null,23,52,null,null,null,null,0), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,6,2018-12-22 8:19:54,null,-1,-1,70,76,null,null,0), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,8,2018-12-22 8:15:29,null,-1,-1,null,null,46,4,1), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,9,2018-12-22 8:40:03,null,-1,-1,null,null,63,10,6), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,4,2018-12-22 8:02:30,联想笔记本,-1,-1,null,null,null,null,2), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,0,2018-12-22 8:13:38,苹果,-1,-1,null,null,null,null,6), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,2,2018-12-22 8:48:54,null,-1,-1,81,19,null,null,6), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,6,2018-12-22 8:23:26,null,-1,-1,null,null,5,55,0), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,0,2018-12-22 8:47:57,null,-1,-1,null,null,31,33,3), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,0,2018-12-22 8:26:44,null,-1,-1,null,null,20,90,5), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,3,2018-12-22 8:39:19,null,46,33,null,null,null,null,2), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,2,2018-12-22 8:32:14,null,-1,-1,null,null,6,2,2), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,8,2018-12-22 8:25:50,null,-1,-1,null,null,96,43,6), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,6,2018-12-22 8:12:28,null,-1,-1,null,null,18,32,6), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,3,2018-12-22 8:28:19,null,-1,-1,48,89,null,null,4), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,9,2018-12-22 8:51:00,null,-1,-1,89,37,null,null,9), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,7,2018-12-22 8:55:11,null,64,61,null,null,null,null,8), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,8,2018-12-22 8:47:17,null,44,16,null,null,null,null,0), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,6,2018-12-22 8:06:58,null,85,45,null,null,null,null,9), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,4,2018-12-22 8:16:20,null,-1,-1,96,67,null,null,2), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,6,2018-12-22 8:25:54,null,-1,-1,null,null,22,51,9), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,4,2018-12-22 8:12:17,null,-1,-1,69,18,null,null,1), UserVisitAction(2018-12-22,20,f3f5404fa2754b5f9fdad3ee4967e7c5,3,2018-12-22 8:37:51,联想笔记本,-1,-1,null,null,null,null,1)))

    //写入数据库并返回top10相关信息，以供需求四调用
    val top10CategoriesArr = getTop10Categories(sparkSession, taskUUID, sessionid2FilterActionRDD)
    /** *********************需求三至此结束，以下开始需求四 ******************************/
    //点击过top商品的action
    val clickForTop10Session = sessionid2FilterActionRDD.filter {
      case (sessionID, action) =>
        top10CategoriesArr.contains(action.click_category_id)
    }

    val clickSession = clickForTop10Session.map {
      case (sessionID, action) =>
        (action.click_category_id, (sessionID, 1l))
    }
    //(16,(3477b99b07a04acb84df5e907682b9d2,1))
    //(61,(3477b99b07a04acb84df5e907682b9d2,1))
    //(70,(3890ef9ca24c4311833a27bee1f7067c,1))
    getTop10Session(sparkSession, taskUUID, clickSession)


  }

  /**
    * 获取过滤文件并返回过滤后的数据
    *
    * @param taskParam             过滤条件
    * @param sessionID2FullInfoRDD 原始文件
    **/
  def getFilterData(taskParam: JSONObject,
                    sessionID2FullInfoRDD: RDD[(String, String)]) = {

    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keyWord = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val category = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keyWord != null) Constants.PARAM_KEYWORDS + "=" + keyWord + "|" else "") +
      (if (category != null) Constants.PARAM_CATEGORY_IDS + "=" + category + "|" else "")

    if (filterInfo.endsWith("|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)

    val sessionID2FilterRDD = sessionID2FullInfoRDD.filter {
      case (sessionID, fullInfo) => {
        var flag = true
        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
          flag = false
        if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS))
          flag = false
        if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES))
          flag = false
        if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX))
          flag = false
        if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS))
          flag = false
        if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS))
          flag = false
        flag
      }
    }
    sessionID2FilterRDD
  }

  /**
    * @param sparkSession
    * @param taskUUID 当前任务id
    * @param sessionid2FilterActionRDD
    * */
  def getTop10Categories(sparkSession: SparkSession,
                         taskUUID: String,
                         sessionid2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    //获取所有商品的cid
    val cid2CidRDD = sessionid2FilterActionRDD.flatMap {
      case (sessionID, action) =>
        val categoryBuffer = new ArrayBuffer[(Long, Long)]()
        if (action.click_category_id != -1) {
          categoryBuffer.append((action.click_category_id, action.click_category_id))
        } else if (action.order_category_ids != null) {
          for (orderID <- action.order_category_ids.split(",")) {
            categoryBuffer.append((orderID.toLong, orderID.toLong))
          }
        } else if (action.pay_category_ids != null) {
          for (payID <- action.pay_category_ids.split(",")) {
            categoryBuffer.append((payID.toLong, payID.toLong))
          }
        }
        categoryBuffer
    }
    //去重
    val cid2CidRDDm = cid2CidRDD.distinct()
    // 获取三个数据量的RDD
    val clickCount = getClickCount(sessionid2FilterActionRDD)
    val orderCount = getOrderCount(sessionid2FilterActionRDD)
    val payCount = getPayCount(sessionid2FilterActionRDD)

    val fullCount = getFullCount(cid2CidRDDm, clickCount, orderCount, payCount)

    val sortKeyFullInfo = fullCount.map {
      case (cid, countInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong
        val sortKeyT = SortKey(clickCount, orderCount, payCount)
        (sortKeyT, countInfo)
    }
    //false 参数则降序，不传参默认升序
    //(SortKey(89,68,78),categoryid=66|clickCount=89|orderCount=68|payCount=78)
    val top10CateArr = sortKeyFullInfo.sortByKey(false).take(10)

    /*    val top10CateRDD = sparkSession.sparkContext.makeRDD(top10CateArr).map {
          case (sortKey, countInfo) =>
            val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
            val clickCount = sortKey.clickCount
            val orderCount = sortKey.orderCount
            val payCount = sortKey.payCount

            Top10Category(taskUUID, cid, clickCount, orderCount, payCount)
        }

        import sparkSession.implicits._
        top10CateRDD.toDF().write.format("jdbc")
          .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
          .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
          .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
          .option("dbtable", "top10_category")
          .mode(SaveMode.Append)
          .save()*/

    top10CateArr.map(item => StringUtils.getFieldFromConcatString(item._2, "\\|", "categoryid").toLong)

  }


  def getClickCount(sessionid2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val clickActionRDD = sessionid2FilterActionRDD.filter(item => item._2.click_category_id != 1)
    val cid21ClickActionRDD = clickActionRDD.map {
      case (sessionID, action) =>
        (action.click_category_id, 1L)
    }
    cid21ClickActionRDD.reduceByKey(_ + _)
  }

  def getOrderCount(sessionid2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val orderActionRDD = sessionid2FilterActionRDD.filter(item => item._2.order_category_ids != null)
    val cid21OrderActionRDD = orderActionRDD.flatMap {
      case (sessionID, action) =>
        action.order_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    cid21OrderActionRDD.reduceByKey(_ + _)
  }

  def getPayCount(sessionid2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val payActionRDD = sessionid2FilterActionRDD.filter(item => item._2.pay_category_ids != null)
    val payNum = payActionRDD.flatMap {
      case (sessionID, action) =>
        action.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    payNum.reduceByKey(_ + _)
  }

  def getFullCount(cid2CidRDD: RDD[(Long, Long)],
                   clickCountRDD: RDD[(Long, Long)],
                   orderCountRDD: RDD[(Long, Long)],
                   payCountRDD: RDD[(Long, Long)]) = {

    val clickInfoRDD = cid2CidRDD.leftOuterJoin(clickCountRDD).map {
      case (cid1, (cid2, option)) =>
        val clickCount = if (option.isDefined) option.get else 0L
        val clickInfo = Constants.FIELD_CATEGORY_ID + "=" + cid1 + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount
        (cid1, clickInfo)
    }

    val orderInfoRDD = clickInfoRDD.leftOuterJoin(orderCountRDD).map {
      case (cid1, (clickInfo, option)) =>
        val orderCount = if (option.isDefined) option.get else 0L
        val orderInfo = clickInfo + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount
        (cid1, orderInfo)
    }

    val fullCountInfo = orderInfoRDD.leftOuterJoin(payCountRDD).map {
      case (cid1, (orderInfo, option)) =>
        val payCount = if (option.isDefined) option.get else 0
        val payInfo = orderInfo + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount
        (cid1, payInfo)
    }
    fullCountInfo
  }

  def getTop10Session(sparkSession: SparkSession, taskUUID: String, clickSession: RDD[(Long, (String, Long))]) = {

    val cid_sessionCount = clickSession.groupByKey().map {
      case (cid, iterable) =>
        val sessionCountMap = scala.collection.mutable.HashMap[String, Long]()
        for ((session, 1) <- iterable) {
          if (!sessionCountMap.contains(session)) {
            sessionCountMap += (session -> 0l)
          }
          sessionCountMap(session) = sessionCountMap(session) + 1
        }
        (cid, sessionCountMap)
    }.collect()

    cid_sessionCount.length
    val top10Session = cid_sessionCount.map {
      case (cid, sessionCountMap) =>
        (cid, sessionCountMap.toList.sortBy(-_._2).take(10))
    }

    val top10Arr = ArrayBuffer[Top10Session]()
    top10Session.map {
      case (cid, sessionCountList) =>
        for ((session, count) <- sessionCountList) {
          top10Arr.append(Top10Session(taskUUID, cid, session, count))
        }
    }
    val top10RDD = sparkSession.sparkContext.makeRDD(top10Arr)

    import sparkSession.implicits._
    top10RDD.toDF().write.format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_session")
      .mode(SaveMode.Append)
      .save()
  }


}
