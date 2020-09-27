package com.ng.bigdata.util

import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer

/**
 * @User: kaisy
 * @Date: 2020/9/24 19:00
 * @Desc:
 */
object TagUtils extends Tag {
  /**
   * 获取广告位标签
   * @param row
   * @return
   */
  override def getAdTags(row: Any*) = {
    // todo 创建可变集合
    val arr = new ArrayBuffer[(String, Int)]()
    // todo 类型转换
    val row1: Row = row(0).asInstanceOf[Row]
    // todo 获取广告位ID及名称
    val id: Int = row1.getAs[Int]("adspacetype")

    // todo 打广告ID标签 非空判断
    if (StringUtils.isNotBlank(id.toString)) {
      id match {
        // tip 模式匹配上之后将数据转换成元组添加进数组
        case id if id > 0 && id <= 9 => arr.append(("LC0" + id, 1))
        case id => arr.append(("LC" + id, 1))
      }
    }
    val name: String = row1.getAs[String]("adspacetypename")
    if (StringUtils.isNotBlank(name)) arr.append(("LN" + name, 1))

    arr
  }

  /**
   * 获取App标签
   * @param row
   * @return
   */
  override def getAppTags(row: Any*): ArrayBuffer[(String, Int)] = {
    // todo 创建App集合
    val appArr = new ArrayBuffer[(String, Int)]()
    // todo 转换类型
    val row1: Row = row(0).asInstanceOf[Row]
    val dict: Broadcast[collection.Map[String, String]] = row(1).asInstanceOf[Broadcast[collection.Map[String, String]]]
    // todo 获取字段
    val appId: String = row1.getAs[String]("appid")
    var appName: String = row1.getAs[String]("appname")
    // todo 判断字段不为空
    if(StringUtils.isNotBlank(appName)){
      appName = dict.value.getOrElse(appId,"未知")
      appArr.append(("APP"+appName,1))
    }
    appArr
  }

  /**
   * 获取渠道地域标签
   * @param row
   * @return
   */
  override def getLocationTags(row: Any*): ArrayBuffer[(String, Int)] = {
    // todo 创建地域集合
    val locationArr = new ArrayBuffer[(String, Int)]()
    // todo 转换类型
    val row1: Row = row(0).asInstanceOf[Row]
    // todo 获取渠道及地域字段
    val apf: Int = row1.getAs[Int]("adplatformproviderid")
    val pro: String = row1.getAs[String]("provincename")
    val city: String = row1.getAs[String]("cityname")
    // todo 非空判断，若非空添加进标签集合
    if(StringUtils.isNotBlank(apf.toString)){
      locationArr.append(("CN"+apf,1))
    }
    if(StringUtils.isNotBlank(pro) & StringUtils.isNotBlank(city)){
      locationArr.append(("ZP"+pro,1))
      locationArr.append(("ZC"+city,1))
    }
    // todo 返回集合
    locationArr
  }

  /**
   * 获取关键字标签
   * @param row
   * @return
   */
  override def getKeyWordTags(row: Any*): ArrayBuffer[(String, Int)] = {
    // todo 创建关键字集合
    val kwArr = new ArrayBuffer[(String, Int)]()
    // todo 类型转换
    val row1: Row = row(0).asInstanceOf[Row]
    // todo 获取关键字字段
    val kw: String = row1.getAs[String]("keywords")
    // todo 获取停用词库
    val list = List("国产动画电影","日本动画电影","日泰恐怖电影","欧美科幻动作","抗日神剧","韩国偶像伦理","惊悚悬疑")

    // todo 切分字段、过滤处理、加入集合
    kw.split("\\|")
      .filter( v => v.length >= 3 && v.length <= 8 && !list.contains(v))
      .foreach( v => {
        kwArr.append(("K"+v,1))
      })

    kwArr
  }

  /**
   * 获取设备标签
   * @param row
   * @return
   */
  override def getDeviceTags(row: Any*): ArrayBuffer[(String, Int)] = {
    // todo 创建设备集合
    val devArr = new ArrayBuffer[(String, Int)]()
    // todo 类型转换
    val row1: Row = row(0).asInstanceOf[Row]
    // todo 获取设备标签字段
    val client: Int = row1.getAs[Int]("client")
    val ispName: String = row1.getAs[String]("ispname")
    val netWork: String = row1.getAs[String]("networkmannername")

    // todo 模式匹配，若匹配上，则按照相应规则添加进集合
    // tip 设备操作系统
    client match{
      case 1 => devArr.append(("D00010001",1))
      case 2 => devArr.append(("D00010002",1))
      case 3 => devArr.append(("D00010003",1))
      case _ => devArr.append(("D00010004",1))
    }
    // tip 设备联网方式
    ispName match {
      case "wifi" => devArr.append(("D00020001",1))
      case "4G" => devArr.append(("D00020002",1))
      case "3G" => devArr.append(("D00020003",1))
      case "2G" => devArr.append(("D00020004",1))
      case _ => devArr.append(("D00020005",1))
    }
    // tip 设备运营方式
    netWork match {
      case "移动" => devArr.append(("D00030001",1))
      case "联通" => devArr.append(("D00030002",1))
      case "电信" => devArr.append(("D00030003",1))
      case _ => devArr.append(("D00030004",1))
    }

    devArr
  }

  /**
   * 获取商圈标签
   * @param row
   * @return
   */
  override def getCircleTags(row: Any*): ArrayBuffer[(String, Int)] = {
    // todo 创建商圈集合
    val arr = new ArrayBuffer[(String, Int)]()
    // todo 类型转换
    val row1: Row = row(0).asInstanceOf[Row]
    // todo 获取经纬度
    val long: Double = String2Utils.str2Double(row1.getAs[String]("longs"))
    val lat: Double = String2Utils.str2Double(row1.getAs[String]("lat"))
    // todo 判断经纬度是否满足经纬度要求
    if(lat>=3 && lat <= 54 && long<= 136 && long >= 73 ){
      // todo 使用地图网络接口的逆地址服务获取商圈
      val circle: String = AmapUtils.getBusinessFromAmap(long, lat)
      // todo 判断商圈是否为空
      if(StringUtils.isNotBlank(circle)){
        if(circle.contains("|")){
          val arr2: Array[String] = circle.split("|")
          arr2.foreach( t => {
            arr.append((t,1))
          })
        }else{
          arr.append((circle,1))
        }
      }
    }
    arr
  }
}
