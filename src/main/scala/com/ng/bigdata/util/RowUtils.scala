package com.ng.bigdata.util

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer

/**
 * @User: kaisy
 * @Date: 2020/9/24 15:32
 * @Desc: row数据处理工具类
 */
object RowUtils {
  /**
   * 获取所有的userId
   * @param row
   */
  def getAllUserId(row: Row) = {
    // todo 创建那
    val arr = new ArrayBuffer[String]()
    if(StringUtils.isNotBlank(row.getAs[String]("imei"))){
      arr.append(row.getAs[String]("imei"))
    }
    if(StringUtils.isNotBlank(row.getAs[String]("mac"))){
      arr.append(row.getAs[String]("mac"))
    }
    if(StringUtils.isNotBlank(row.getAs[String]("idfa"))){
      arr.append(row.getAs[String]("idfa"))
    }
    if(StringUtils.isNotBlank(row.getAs[String]("openudid"))){
      arr.append(row.getAs[String]("openudid"))
    }
    if(StringUtils.isNotBlank(row.getAs[String]("androidid"))){
      arr.append(row.getAs[String]("androidid"))
    }
    arr
  }

  /**
   * 获取设备名称
   *
   * @param row
   * @param str
   * @return
   */
  def getOperatorName(row: Row, str: String): String = {
    // todo 获取操作系统代号
    val operatorType: Int = row.getAs[Int](str)
    // todo 模式匹配操作系统代号，得到操作系统名称
    operatorType match{
      case 1 => "android"
      case 2 => "ios"
      case 3 => "wp"
      case _ => "other"
    }

  }

  /**
   * 获取设备名称
   * @param row
   * @param str
   * @return
   */
  def getDeviceName(row: Row, str: String): String = {
    // todo 获取设备类别代号
    val deviceType: Int = row.getAs[Int](str)
    // todo 模式匹配类别代号，得到设备名称
    deviceType match{
      case 1 => "手机"
      case 2 => "平板"
      case _ => "其他"
    }
  }

  /**
   * 获取唯一不为空的ID
   * @param row
   */
  def getUserID(row: Row) = {
    // todo 模式匹配获取ID
    row match {
      case r if StringUtils.isNotBlank(r.getAs[String]("imei")) => r.getAs[String]("imei")
      case r if StringUtils.isNotBlank(r.getAs[String]("mac")) => r.getAs[String]("mac")
      case r if StringUtils.isNotBlank(r.getAs[String]("idfa")) => r.getAs[String]("idfa")
      case r if StringUtils.isNotBlank(r.getAs[String]("openudid")) => r.getAs[String]("openudid")
      case r if StringUtils.isNotBlank(r.getAs[String]("androidid")) => r.getAs[String]("androidid")
      case _ => ""
    }
  }

  /**
   * 指标计算
   * @param t
   */
  def countUtils(t: Row) = {
    // todo 定义变量
    var origin_cnt = 0
    var effective_cnt = 0
    var ad_request = 0
    var rtb_cnt = 0
    var  win_cnt =0
    var exhibit_cnt = 0
    var click_cnt =0
    var ad_consume =0.0
    var ad_cost = 0.0

    // todo 判断条件
    if(t.getAs[Int]("requestmode") == 1 &&
      t.getAs[Int]("processnode") >= 1){
      // tip 原始请求数
      origin_cnt = 1
    }
    if(t.getAs[Int]("requestmode") == 1 &&
      t.getAs[Int]("processnode") >= 2){
      // tip 有效广告数
      effective_cnt = 1
    }
    if(t.getAs[Int]("requestmode") == 1 &&
      t.getAs[Int]("processnode") == 3){
      // tip 广告请求数
      ad_request = 1
    }
    if(t.getAs[Int]("iseffective") == 1 &&
      t.getAs[Int]("isbilling") == 1){
      if(t.getAs[Int]("isbid") == 1){
        // tip 参与竞价数
        rtb_cnt = 1
      }
      if(t.getAs[Int]("iswin") == 1 &&
        t.getAs[Int]("adorderid") == 1){
        // tip 竞价成功数
        win_cnt = 1
        // tip DSP广告消费
        ad_consume = t.getAs[Double]("winprice") / 1000
        // tip DSP广告成本
        ad_cost = t.getAs[Double]("adpayment") / 1000
      }
      if(t.getAs[Int]("requestmode") == 2 &&
        t.getAs[Int]("iseffective") == 1){
        // tip 展示数
        exhibit_cnt = 1
      }
      if(t.getAs[Int]("requestmode") == 2 &&
        t.getAs[Int]("iseffective") == 1){
        // tip 点击数
        click_cnt = 1
      }
    }
    // todo 返回封装指标集合
    List[Double](origin_cnt,
      effective_cnt,
      ad_request,
      rtb_cnt,
      win_cnt,
      exhibit_cnt,
      click_cnt,
      ad_consume,
      ad_cost)
  }
}
