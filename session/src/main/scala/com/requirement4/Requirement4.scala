package com.requirement4

import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.{ParamUtils, ValidUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class Requirement4 extends java.io.Serializable {


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

}
