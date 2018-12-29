package com.requirement1

import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{SessionAggrStat, UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable


class Requirement1 extends java.io.Serializable {
  def doRequirement1() = {
    //创建配置对象
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    //此处引用的包为“net.sf.json.JSONObject”
    //将字符串转化为JSON对象，以便于下一步处理
    //此处的JSON对象用于数据初筛选，筛选条件在该JSON字符串中可做修改
    val taskParams = JSONObject.fromObject(jsonStr)

    //生成UUID作为当前任务的ID
    val taskUUID = UUID.randomUUID().toString
    //获取sparkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("session")
    //获取sparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //获取当前初筛选条件下的所有userVisitAction组成的RDD，数据内容如下
    //(日期，用户ID，sessionID，页面ID，动作时间，搜索关键字，点击类别ID，点击产品ID，订单产品ID集合，付款类别ID集合，付款产品ID集合，城市ID)
    //UserVisitAction(2018-12-18,68,960ada8a169f41fcbc55b27a9b4a6271,5,2018-12-18 3:30:57,null,-1,-1,89,66,null,null,2)
    //UserVisitAction(2018-12-18,68,960ada8a169f41fcbc55b27a9b4a6271,7,2018-12-18 3:48:36,null,68,92,null,null,null,null,9)
    //UserVisitAction(2018-12-18,68,960ada8a169f41fcbc55b27a9b4a6271,9,2018-12-18 3:11:24,null,6,35,null,null,null,null,9)
    //UserVisitAction(2018-12-18,68,960ada8a169f41fcbc55b27a9b4a6271,6,2018-12-18 3:35:25,null,-1,-1,null,null,27,22,4)
    val userVisitActionsRDD = getActiveRDD(sparkSession, taskParams)

    //将userVisitActionRDD中的每个userVisitAction转换为(userVisitAction.session_id,userVisitAction)的二元组
    val actionsTypeBySession = userVisitActionsRDD.map(userVisitAction => (userVisitAction.session_id, userVisitAction))
    //groupbykey，将actionsTypeBySession转换为斧形数据(k,iteratable)
    /*3f4c709731f843b58707758c161cc814,CompactBuffer(
    UserVisitAction(2018-12-18,43,3f4c709731f843b58707758c161cc814,0,2018-12-18 20:08:17,null,-1,-1,88,54,null,null,5),
    UserVisitAction(2018-12-18,43,3f4c709731f843b58707758c161cc814,8,2018-12-18 20:45:00,null,99,87,null,null,null,null,0),
    UserVisitAction(2018-12-18,43,3f4c709731f843b58707758c161cc814,9,2018-12-18 20:14:56,保温杯,-1,-1,null,null,null,null,4),
    UserVisitAction(2018-12-18,43,3f4c709731f843b58707758c161cc814,2,2018-12-18 20:49:05,null,-1,-1,null,null,36,9,0))*/
    val actionsGroupByKey = actionsTypeBySession.groupByKey()
    //持久化
    actionsGroupByKey.cache()

    val sessionID2FullInfoRDD = getFullInfoData(sparkSession, actionsGroupByKey)
    //sessionID2FullInfoRDD数据没问题

    //创建累加器对象
    val sessionStatAccumulator = new SessionStatAccumulator
    //将累加器在sc中注册
    sparkSession.sparkContext.register(sessionStatAccumulator, "sessionStatAccumulator")
    //将数据过滤并由计数器记录
    getFilterData(taskParams, sessionStatAccumulator, sessionID2FullInfoRDD)
    //将计数器数据写入mysql
    getFinalData(sparkSession, taskUUID, sessionStatAccumulator.value)
  }


  /**
    * @ 筛选数据并将数据封装进RDD
    *
    * @param sparkSession
    * @param taskParams JSON对象，用以保存筛选条件
    * @return 新的RDD[UserVisitAction]
    **/
  def getActiveRDD(sparkSession: SparkSession, taskParams: JSONObject) = {
    //获取筛选的开始日期
    val startDate = ParamUtils.getParam(taskParams, Constants.PARAM_START_DATE)
    //获取筛选的结束日期
    val endDate = ParamUtils.getParam(taskParams, Constants.PARAM_END_DATE)
    //根据日期查询
    val sql = "select * from user_visit_action where date >='" + startDate + "' and date <='" + endDate + "'"
    import sparkSession.implicits._
    //通过sparksession在hive数据库中查询
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

  /**
    * @param sparkSession
    * @param actionsIterator RDD[(sessionID,iterableAction)]
    *                        @
    **/
  def getFullInfoData(sparkSession: SparkSession, actionsIterator: RDD[(String, scala.Iterable[UserVisitAction])]) = {
    //对actionsGroupByKey的数据进行
    val userID2AggrInfoRDD = actionsIterator.map {
      case (sessionID, iterableAction) =>
        var startTime: Date = null
        var endTime: Date = null
        var userID = -1L
        var searchKeyWords = new StringBuilder("")
        var clickCategories = new StringBuilder("")
        var stepLenth = 0
        for (action <- iterableAction) {
          if (userID == -1L) {
            userID = action.user_id
          }
          val actionTime = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }
          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }
          val searchKeyWord = action.search_keyword
          val clickCategory = action.click_category_id
          if (StringUtils.isNotEmpty(searchKeyWord) && !searchKeyWords.toString.contains(searchKeyWord)) {
            searchKeyWords.append(searchKeyWord)
          }
          if (clickCategory != -1L && !clickCategories.toString.contains(clickCategory)) {
            clickCategories.append(clickCategory)
          }
          stepLenth += 1
        }

        val searchKW = StringUtils.trimComma(searchKeyWords.toString)
        val clickCate = StringUtils.trimComma(clickCategories.toString)

        val visitLenth = (endTime.getTime - startTime.getTime) / 1000

        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionID + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKW + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCate + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLenth + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLenth + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        (userID, aggrInfo)
    }

    userID2AggrInfoRDD.collect()
    //userID2AggrInfoRDD.foreach(println(_))

    val sql = "select * from user_info"
    import sparkSession.implicits._
    val userInfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(userInfo => (userInfo.user_id, userInfo))

    val sessionld2FulInfoRDD = userID2AggrInfoRDD.join(userInfoRDD).map {
      case (userId, (aggrInfo, userInfo)) =>
        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, delimiter = "\\|", Constants.FIELD_SESSION_ID)
        val fullInfo = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + userInfo.age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
          Constants.FIELD_SEX + "=" + userInfo.sex + "|" +
          Constants.FIELD_CITY + "=" + userInfo.city
        (sessionId, fullInfo)
    }
    sessionld2FulInfoRDD.collect()
    sessionld2FulInfoRDD
  }

  def getFilterData(taskParam: JSONObject,
                    sessionStatAccumulator: SessionStatAccumulator,
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

        if (flag) {
          sessionStatAccumulator.add(Constants.SESSION_COUNT)
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          calculateVisitLength(visitLength, sessionStatAccumulator)
          calculateStepLength(stepLength, sessionStatAccumulator)
        }
        flag
      }
    }
    sessionID2FilterRDD.collect()
    sessionID2FilterRDD
  }

  //步长计数器规则
  def calculateStepLength(stepLength: Long, sessionStatAccumulator: SessionStatAccumulator) = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength >= 30 && stepLength <= 60) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  //时长计数器规则
  def calculateVisitLength(visitLength: Long, sessionStatAccumulator: SessionStatAccumulator) = {

    if (visitLength >= 1 && visitLength <= 3) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength >= 60 && visitLength <= 180) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength >= 181 && visitLength <= 600) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength >= 610 && visitLength <= 1800) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  def getFinalData(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {

    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble
    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_lm_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val sta = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio,
      visit_length_7s_9s_ratio, visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_lm_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val statRDD = sparkSession.sparkContext.makeRDD(Array(sta))
    import sparkSession.implicits._
    statRDD.toDF().write.format("jdbc").option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_aggr_stat")
      .mode(SaveMode.Append)
      .save()
  }
}
