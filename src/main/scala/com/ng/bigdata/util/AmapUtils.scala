package com.ng.bigdata.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * @User: kaisy
 * @Date: 2020/9/27 10:59
 * @Desc:
 */
object AmapUtils {
  /**
   * 发送URL请求，获取商圈
   * @param long
   * @param lat
   * @return
   */
  def getBusinessFromAmap(long: Double, lat: Double): String = {
    // todo 拼接经纬字符串
    val location: String = long + "," + lat
    // todo 获取web端接口服务url
    val url: String = "https://restapi.amap.com/v3/geocode/regeo?key=45e8262ebc7d74d15afa6fb2e10163dd&location="+ location

    // todo 调用http发送请求接口
    val circle: String = HttpUtils.get(url)
    // todo 解析json字符串
    val json: JSONObject = JSON.parseObject(circle)
    // tip 判断是否为正确数据
    val status: String = json.getString("status")
    if(status.equals("0")) return ""
    // tip 逐层解析
    val regeocode: JSONObject = json.getJSONObject("regeocode")
    if(regeocode == null) return ""
    val addressComponent: JSONObject = regeocode.getJSONObject("addressComponent")
    if(addressComponent == null) return ""
    val businessAreas: JSONArray = addressComponent.getJSONArray("businessAreas")
    if(businessAreas.isEmpty && businessAreas == null) return ""
    // todo 定义一个list集合
    val list = new ListBuffer[String]()
    // todo 循环遍历获取json商圈名字
    for(json<-businessAreas.toArray){
      if (json.isInstanceOf[JSONObject]){
        val name: String = json.asInstanceOf[JSONObject].getString("name")
        list.append(name)
      }
    }
    list.mkString("|")
  }

  def main(args: Array[String]): Unit = {
    println(getBusinessFromAmap(116.481488, 39.990464))
  }
}
