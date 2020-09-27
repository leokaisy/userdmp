package com.ng.bigdata.util

/**
 * @User: kaisy
 * @Date: 2020/9/23 15:01
 * @Desc:
 */
object String2Utils {
  def str2Int(str: String) = {
    try{
      str.toInt
    }catch{
      case _:Exception => 0
    }
  }
  def str2Double(str: String) = {
    try{
      str.toDouble
    }catch{
      case _:Exception => 0.0
    }
  }
}
