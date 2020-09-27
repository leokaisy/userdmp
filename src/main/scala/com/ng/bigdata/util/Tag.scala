package com.ng.bigdata.util

import scala.collection.mutable.ArrayBuffer

/**
 * @User: kaisy
 * @Date: 2020/9/24 19:19
 * @Desc: 标签公共接口
 */
trait Tag {
   /**
    * 获取广告位标签
    * @param row
    * @return
    */
   def getAdTags(row:Any*):ArrayBuffer[(String,Int)]

   /**
    * 获取App标签
    * @param row
    * @return
    */
   def getAppTags(row:Any*):ArrayBuffer[(String,Int)]

   /**
    * 获取渠道地域标签
    * @param row
    * @return
    */
   def getLocationTags(row:Any*):ArrayBuffer[(String,Int)]

   /**
    * 获取关键字标签
    * @param row
    * @return
    */
   def getKeyWordTags(row:Any*):ArrayBuffer[(String,Int)]

   /**
    * 获取设备标签
    * @param row
    * @return
    */
   def getDeviceTags(row:Any*):ArrayBuffer[(String,Int)]

   /**
    * 获取设备标签
    * @param row
    * @return
    */
   def getCircleTags(row:Any*):ArrayBuffer[(String,Int)]

}
