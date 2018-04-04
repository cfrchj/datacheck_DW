package cn.ac.iie.iaa

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import DAO.{HBaseDAO, KafkaDAO, MysqlDAO}
import Model.ConfPara
import org.apache.spark.broadcast.Broadcast

object SparkDatacheckDW extends Serializable {
  case class DNSlog1(DD_NM_VALID: String, DD_TIME_COLLECT_MIN: String, IP_TARGET_FIRST: String, USER_TYPE: String, PROVINCE: String) // 源IP、有效域名、统计周期（分）、取第一个目的IP、、用户类型、解析类型  、上传省份
  case class DNSlog2(KEY: String, COUNT: Long, DAY: String)//key(五分钟划分、时间（小时）、域名、泛域名、域名级别、目的IP、用户类型、上传省份代码)
  /*取泛域名以及域名级别、结合成单一字符串*/
  def KeyMergeResult(combine: (DNSlog1, Int)): DNSlog2 = {
    val Day = combine._1.DD_TIME_COLLECT_MIN.substring(3, 11)
    val domainStr = combine._1.DD_NM_VALID//取泛域名以及域名级别
    //var mainDomain = InternalProcess.getDomainInfo(domainStr, "domain")
    //if (mainDomain == "-1") mainDomain = domainStr
    val mainDomain = "NULL"
    //val domainLevel = InternalProcess.getDomainInfo(domainStr, "level")
    val domainLevel = "3"
    //增加五分钟标志(Min)     
    val resultKey = combine._1.DD_TIME_COLLECT_MIN + "|" + combine._1.DD_NM_VALID + "|" + mainDomain + "|" + domainLevel + "|" + combine._1.IP_TARGET_FIRST + "|" + combine._1.USER_TYPE + "|" + combine._1.PROVINCE
    DNSlog2(resultKey, combine._2, Day)
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: cn.ac.iie.iaa.SparkDatacheckDW <dataCheck-DW-config-file>")
      System.exit(1)
    }

    val cp = new ConfPara( args(0) )
    val SparkConf = new SparkConf().setAppName("DataCheckDW_" + cp.provinceLongID)
    val ssc = new StreamingContext(SparkConf, Seconds(cp.batchDuration))

    var provinceLongID = cp.provinceLongID
    if (cp.provinceLongID == "1") provinceLongID = "750000"

    val ipControl: Array[(Long, Long, String)] = MysqlDAO.getIPControlTable(cp, provinceLongID, ssc)
    val ipCtrBroadcast: Broadcast[Array[(Long, Long, String)]] = ssc.sparkContext.broadcast[ Array[(Long, Long, String)] ](ipControl) //广播mysql数据表

    val srcData = KafkaDAO.directReadDataFromKafka(cp, ssc)
    srcData.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val pairs = rdd.map( line => ( line._2, line._2.split(cp.srcSplitPattern) ) )
        val checkResult = pairs.map(pair => (pair._1,pair._2,
          if (pair._2.length >= cp.splitLimitNum &&
            cp.formula0.matcher(pair._2(0)).matches &&
            cp.formula1.matcher(pair._2(1)).matches &&
            cp.formula2.matcher(pair._2(2)).matches &&
            cp.formula3.matcher(pair._2(3)).matches) 1 else 0))
        val correctData = checkResult.filter { case (_, _, flag) => flag == 1 }
        val tmpp: Array[(Long, Long, String)] = ipCtrBroadcast.value
        val DNSLOG_PARSE = correctData.map { str =>
          val userType = InternalProcess.getUserType(str._2(0), tmpp)
          val validDomain = InternalProcess.ValidDomian(str._2(1))
          val timeOrigin = str._2(2)
          val timeMin = InternalProcess.ComputeMin(timeOrigin.substring(10, 12)) + "|" + timeOrigin.substring(0, 10)
          val IPTarget = InternalProcess.GetFirstIP(str._2(3))
          DNSlog1(validDomain, timeMin, IPTarget, userType, provinceLongID)          //domain、time、destIP、userType、province
        }
        val DNSLOG_F = DNSLOG_PARSE.filter(line => line.USER_TYPE != "-1" && line.DD_NM_VALID != "0") //清除无效记录       
        val DNSLOG_ADD = DNSLOG_F.map(log => (log, 1))
        val DNSLOG_FOLD = DNSLOG_ADD.foldByKey(0)(_ + _)
        val DNSLOG_GRP = DNSLOG_FOLD .reduceByKey(_ + _)
        val DNSLOG_KEYMERGE = DNSLOG_GRP.map(KeyMergeResult)

        HBaseDAO.writeRDDToHBase(DNSLOG_KEYMERGE, cp)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
