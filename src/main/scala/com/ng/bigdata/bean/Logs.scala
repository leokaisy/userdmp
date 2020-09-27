package com.ng.bigdata.bean

/**
 * @User: kaisy
 * @Date: 2020/9/23 14:55
 * @Desc: 类反射创建df
 */

class Logs(
            val sessionid: String,
            val advertisersid: Int,
            val adorderid: Int,
            val adcreativeid: Int,
            val adplatformproviderid: Int,
            val sdkversion: String,
            val adplatformkey: String,
            val putinmodeltype: Int,
            val requestmode: Int,
            val adprice: Double,
            val adppprice: Double,
            val requestdate: String,
            val ip: String,
            val appid: String,
            val appname: String,
            val uuid: String,
            val device: String,
            val client: Int,
            val osversion: String,
            val density: String,
            val pw: Int,
            val ph: Int,
            val longs: String, // 可能会有问题
            val lat: String,
            val provincename: String,
            val cityname: String,
            val ispid: Int,
            val ispname: String,
            val networkmannerid: Int,
            val networkmannername:String,
            val iseffective: Int,
            val isbilling: Int,
            val adspacetype: Int,
            val adspacetypename: String,
            val devicetype: Int,
            val processnode: Int,
            val apptype: Int,
            val district: String,
            val paymode: Int,
            val isbid: Int,
            val bidprice: Double,
            val winprice: Double,
            val iswin: Int,
            val cur: String,
            val rate: Double,
            val cnywinprice: Double,
            val imei: String,
            val mac: String,
            val idfa: String,
            val openudid: String,
            val androidid: String,
            val rtbprovince: String,
            val rtbcity: String,
            val rtbdistrict: String,
            val rtbstreet: String,
            val storeurl: String,
            val realip: String,
            val isqualityapp: Int,
            val bidfloor: Double,
            val aw: Int,
            val ah: Int,
            val imeimd5: String,
            val macmd5: String,
            val idfamd5: String,
            val openudidmd5: String,
            val androididmd5: String,
            val imeisha1: String,
            val macsha1: String,
            val idfasha1: String,
            val openudidsha1: String,
            val androididsha1: String,
            val uuidunknow: String,
            val userid: String,
            val iptype: Int,
            val initbidprice: Double,
            val adpayment: Double,
            val agentrate: Double,
            val lomarkrate: Double,
            val adxrate: Double,
            val title: String,
            val keywords: String,
            val tagid: String,
            val callbackdate: String,
            val channelid: String,
            val mediatype: Int
          ) extends Product with Serializable {
  // 映射关系 下标和成员关系
  override def productElement(n: Int): Any = n match {
    case 0=>sessionid
    case 1=>advertisersid
    case 2=>adorderid
    case 3=>adcreativeid
    case 4=>adplatformproviderid
    case 5=>sdkversion
    case 6=>adplatformkey
    case 7=>putinmodeltype
    case 8=>requestmode
    case 9=>adprice
    case 10=>adppprice
    case 11=>requestdate
    case 12=>ip
    case 13=>appid
    case 14=>appname
    case 15=>uuid
    case 16=>device
    case 17=>client
    case 18=>osversion
    case 19=>density
    case 20=>pw
    case 21=>ph
    case 22=>longs  // 可能会有问题
    case 23=>lat
    case 24=>provincename
    case 25=>cityname
    case 26=>ispid
    case 27=>ispname
    case 28=>networkmannerid
    case 29=>networkmannername
    case 30=>iseffective
    case 31=>isbilling
    case 32=>adspacetype
    case 33=>adspacetypename
    case 34=>devicetype
    case 35=>processnode
    case 36=>apptype
    case 37=>district
    case 38=>paymode
    case 39=>isbid
    case 40=>bidprice
    case 41=>winprice
    case 42=>iswin
    case 43=>cur
    case 44=>rate
    case 45=>cnywinprice
    case 46=>imei
    case 47=>mac
    case 48=>idfa
    case 49=>openudid
    case 50=>androidid
    case 51=>rtbprovince
    case 52=>rtbcity
    case 53=>rtbdistrict
    case 54=>rtbstreet
    case 55=>storeurl
    case 56=>realip
    case 57=>isqualityapp
    case 58=>bidfloor
    case 59=>aw
    case 60=>ah
    case 61=>imeimd5
    case 62=>macmd5
    case 63=>idfamd5
    case 64=>openudidmd5
    case 65=>androididmd5
    case 66=>imeisha1
    case 67=>macsha1
    case 68=>idfasha1
    case 69=>openudidsha1
    case 70=>androididsha1
    case 71=>uuidunknow
    case 72=>userid
    case 73=>iptype
    case 74=>initbidprice
    case 75=>adpayment
    case 76=>agentrate
    case 77=>lomarkrate
    case 78=>adxrate
    case 79=>title
    case 80=>keywords
    case 81=>tagid
    case 82=>callbackdate
    case 83=>channelid
    case 84=>mediatype
  }
  // 成员数量
  override def productArity: Int = 85
  override def canEqual(that: Any): Boolean = that.isInstanceOf[Logs]

  override def equals(other: Any): Boolean = other match {
    case that: Logs =>
      (that canEqual this) &&
        sessionid == that.sessionid &&
        advertisersid == that.advertisersid &&
        adorderid == that.adorderid &&
        adcreativeid == that.adcreativeid &&
        adplatformproviderid == that.adplatformproviderid &&
        sdkversion == that.sdkversion &&
        adplatformkey == that.adplatformkey &&
        putinmodeltype == that.putinmodeltype &&
        requestmode == that.requestmode &&
        adprice == that.adprice &&
        adppprice == that.adppprice &&
        requestdate == that.requestdate &&
        ip == that.ip &&
        appid == that.appid &&
        appname == that.appname &&
        uuid == that.uuid &&
        device == that.device &&
        client == that.client &&
        osversion == that.osversion &&
        density == that.density &&
        pw == that.pw &&
        ph == that.ph &&
        longs == that.longs &&
        lat == that.lat &&
        provincename == that.provincename &&
        cityname == that.cityname &&
        ispid == that.ispid &&
        ispname == that.ispname &&
        networkmannerid == that.networkmannerid &&
        networkmannername == that.networkmannername &&
        iseffective == that.iseffective &&
        isbilling == that.isbilling &&
        adspacetype == that.adspacetype &&
        adspacetypename == that.adspacetypename &&
        devicetype == that.devicetype &&
        processnode == that.processnode &&
        apptype == that.apptype &&
        district == that.district &&
        paymode == that.paymode &&
        isbid == that.isbid &&
        bidprice == that.bidprice &&
        winprice == that.winprice &&
        iswin == that.iswin &&
        cur == that.cur &&
        rate == that.rate &&
        cnywinprice == that.cnywinprice &&
        imei == that.imei &&
        mac == that.mac &&
        idfa == that.idfa &&
        openudid == that.openudid &&
        androidid == that.androidid &&
        rtbprovince == that.rtbprovince &&
        rtbcity == that.rtbcity &&
        rtbdistrict == that.rtbdistrict &&
        rtbstreet == that.rtbstreet &&
        storeurl == that.storeurl &&
        realip == that.realip &&
        isqualityapp == that.isqualityapp &&
        bidfloor == that.bidfloor &&
        aw == that.aw &&
        ah == that.ah &&
        imeimd5 == that.imeimd5 &&
        macmd5 == that.macmd5 &&
        idfamd5 == that.idfamd5 &&
        openudidmd5 == that.openudidmd5 &&
        androididmd5 == that.androididmd5 &&
        imeisha1 == that.imeisha1 &&
        macsha1 == that.macsha1 &&
        idfasha1 == that.idfasha1 &&
        openudidsha1 == that.openudidsha1 &&
        androididsha1 == that.androididsha1 &&
        uuidunknow == that.uuidunknow &&
        userid == that.userid &&
        iptype == that.iptype &&
        initbidprice == that.initbidprice &&
        adpayment == that.adpayment &&
        agentrate == that.agentrate &&
        lomarkrate == that.lomarkrate &&
        adxrate == that.adxrate &&
        title == that.title &&
        keywords == that.keywords &&
        tagid == that.tagid &&
        callbackdate == that.callbackdate &&
        channelid == that.channelid &&
        mediatype == that.mediatype
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(sessionid, advertisersid, adorderid, adcreativeid, adplatformproviderid, sdkversion, adplatformkey, putinmodeltype, requestmode, adprice, adppprice, requestdate, ip, appid, appname, uuid, device, client, osversion, density, pw, ph, longs, lat, provincename, cityname, ispid, ispname, networkmannerid, networkmannername, iseffective, isbilling, adspacetype, adspacetypename, devicetype, processnode, apptype, district, paymode, isbid, bidprice, winprice, iswin, cur, rate, cnywinprice, imei, mac, idfa, openudid, androidid, rtbprovince, rtbcity, rtbdistrict, rtbstreet, storeurl, realip, isqualityapp, bidfloor, aw, ah, imeimd5, macmd5, idfamd5, openudidmd5, androididmd5, imeisha1, macsha1, idfasha1, openudidsha1, androididsha1, uuidunknow, userid, iptype, initbidprice, adpayment, agentrate, lomarkrate, adxrate, title, keywords, tagid, callbackdate, channelid, mediatype)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }


}