package com.ng.bigdata.util

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

import scala.collection.mutable.ArrayBuffer

/**
 * @User: kaisy
 * @Date: 2020/9/27 10:59
 * @Desc: 发送http请求
 */
object HttpUtils {
  /**
   * 发送Http请求
   * @param url
   * @return
   */
  def get(url: String): String = {
    // todo 获取客户端对象
    val client: CloseableHttpClient = HttpClients.createDefault()
    // todo 获取get请求对象
    val hGet = new HttpGet(url)
    // todo 发送请求
    val response: CloseableHttpResponse = client.execute(hGet)
    // todo 获取结果，需要设置编码集
    EntityUtils.toString(response.getEntity,"UTF-8")

  }

}
