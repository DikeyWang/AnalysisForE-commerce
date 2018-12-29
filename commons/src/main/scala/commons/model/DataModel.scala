/*
 * Copyright (c) 2018  All Rights Reserved.
 */

package commons.model

//***************** 输入表 *********************

/**
  * 用户访问动作表
  *
  * @param date               用户点击行为的日期
  * @param user_id            用户的ID
  * @param session_id         Session的ID
  * @param page_id            某个页面的ID
  * @param action_time        点击行为的时间点
  * @param search_keyword     用户搜索的关键词
  * @param click_category_id  某一个商品品类的ID
  * @param click_product_id   某一个商品的ID
  * @param order_category_ids 一次订单中所有品类的ID集合
  * @param order_product_ids  一次订单中所有商品的ID集合
  * @param pay_category_ids   一次支付中所有品类的ID集合
  * @param pay_product_ids    一次支付中所有商品的ID集合
  * @param city_id            城市ID
  */
case class UserVisitAction(date: String,
                           user_id: Long,
                           session_id: String,
                           page_id: Long,
                           action_time: String,
                           search_keyword: String,
                           click_category_id: Long,
                           click_product_id: Long,
                           order_category_ids: String,
                           order_product_ids: String,
                           pay_category_ids: String,
                           pay_product_ids: String,
                           city_id: Long
                          )

/**
  * 用户信息表
  *
  * @param user_id      用户的ID
  * @param username     用户的名称
  * @param name         用户的名字
  * @param age          用户的年龄
  * @param professional 用户的职业
  * @param city         用户所在的城市
  * @param sex          用户的性别
  */
case class UserInfo(user_id: Long,
                    username: String,
                    name: String,
                    age: Int,
                    professional: String,
                    city: String,
                    sex: String
                   )

/**
  * 产品表
  *
  * @param product_id   商品的ID
  * @param product_name 商品的名称
  * @param extend_info  商品额外的信息
  */
case class ProductInfo(product_id: Long,
                       product_name: String,
                       extend_info: String
                      )

/**
  * 需求1表
  * 计算步长和时长
  **/
case class SessionAggrStat(task_id: String,
                           session_count: Long,
                           visit_length_1s_3s_ratio: Double,
                           Visit_length_4s_6s_ratio: Double,
                           visit_length_7s_9s_ratio: Double,
                           visit_length_10s_30s_ratio: Double,
                           visit_length_30s_60s_ratio: Double,
                           visit_length_lm_3m_ratio: Double,
                           visit_1ength_3m_10m_ratio: Double,
                           visit_length_10m_30m_ratio: Double,
                           visit_length_30m_ratio: Double,
                           step_length_1_3_ratio: Double,
                           step_length_4_6_ratio: Double,
                           step_length_7_9_ratio: Double,
                           step_1ength_10_30_ratio: Double,
                           step_length_30_60_ratio: Double,
                           step_length_60_ratio: Double
                          )


/** Session随机抽取表
  *
  * @param task_id                       当前计算批次的ID
  * @param session_id                 抽取的Session的ID
  * @param start_time                 Session的开始时间
  * @param search_keywords     Session的查询字段
  * @param click_category_ids     Session点击的类别id集合
  */
case class SessionRandomExtract(task_id:String,
                                session_id:String,
                                start_time:String,
                                search_keywords:String,
                                click_category_ids:String)

/**
  * Session随机抽取详细表
  *
  * @param taskid            当前计算批次的ID
  * @param userid            用户的ID
  * @param sessionid         Session的ID
  * @param pageid            某个页面的ID
  * @param actionTime        点击行为的时间点
  * @param searchKeyword     用户搜索的关键词
  * @param clickCategoryId   某一个商品品类的ID
  * @param clickProductId    某一个商品的ID
  * @param orderCategoryIds  一次订单中所有品类的ID集合
  * @param orderProductIds   一次订单中所有商品的ID集合
  * @param payCategoryIds    一次支付中所有品类的ID集合
  * @param payProductIds     一次支付中所有商品的ID集合
  **/
case class SessionDetail(taskid:String,
                         userid:Long,
                         sessionid:String,
                         pageid:Long,
                         actionTime:String,
                         searchKeyword:String,
                         clickCategoryId:Long,
                         clickProductId:Long,
                         orderCategoryIds:String,
                         orderProductIds:String,
                         payCategoryIds:String,
                         payProductIds:String)
/**
  * 品类Top10表
  * @param task_id
  * @param category_id
  * @param click_count
  * @param order_count
  * @param pay_count
  */
case class Top10Category(task_id:String,
                         category_id:Long,
                         click_count:Long,
                         order_count:Long,
                         pay_count:Long)
