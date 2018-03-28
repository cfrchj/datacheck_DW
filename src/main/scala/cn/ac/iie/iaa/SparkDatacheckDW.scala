package cn.ac.iie.iaa

import java.text.SimpleDateFormat

import scala.xml.XML

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Durability
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.MD5Hash
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.KafkaUtils

import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import java.sql.DriverManager

/*spark streaming 读取kafka数据，检核与轻度汇总之后写入HBASE*/
object SparkDatacheckDW extends Serializable {
  // 源IP、有效域名、统计周期（分）、取第一个目的IP、、用户类型、解析类型  、上传省份
  case class DNSlog1(DD_NM_VALID: String, DD_TIME_COLLECT_MIN: String, IP_TARGET_FIRST: String, USER_TYPE: String, PROVINCE: String)
  //key(五分钟划分、时间（小时）、域名、泛域名、域名级别、目的IP、用户类型、上传省份代码)
  case class DNSlog2(KEY: String, COUNT: Long, DAY: String)

  /*取泛域名以及域名级别、结合成单一字符串*/
  def KeyMergeResult(combine: (DNSlog1, Int)): DNSlog2 = {
    val Day = combine._1.DD_TIME_COLLECT_MIN.substring(3, 11)
    //取泛域名以及域名级别
    val domainStr = combine._1.DD_NM_VALID
    var mainDomain = InternalProcess.getDomainInfo(domainStr, "domain")
    if (mainDomain == "-1") mainDomain = domainStr
    //val domainLevel = InternalProcess.getDomainInfo(domainStr, "level")
    val domainLevel = "NULL"
    //增加五分钟标志(Min)     
    val resultKey = combine._1.DD_TIME_COLLECT_MIN + "|" + combine._1.DD_NM_VALID + "|" + mainDomain + "|" + domainLevel + "|" + combine._1.IP_TARGET_FIRST + "|" + combine._1.USER_TYPE + "|" + combine._1.PROVINCE
    DNSlog2(resultKey, combine._2, Day)
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: cn.ac.iie.iaa.SparkDatacheckDW <data-check-dw-config-file>")
      System.exit(1)
    }

    /*解析XML配置*/
    val xml = XML.loadFile(args(0))
    val srcSplitPattern = "\\" + (xml \\ "src_split_pattern").text
    val splitLimitNum = (xml \\ "split_limit_num").text.toInt
    val batchDuration = (xml \\ "batch_duration").text.toLong
    var provinceLongID = (xml \\ "prov_long_id").text
    val checkResultPath = (xml \\ "check_result_path").text
    val srcRddRepartitionNums = (xml \\ "src_rdd_repartition_nums").text.toInt
    val checkCorrectCoalesceNums = (xml \\ "check_correct_coalesce_nums").text.toInt
    val checkErrorCoalesceNums = (xml \\ "check_error_coalesce_nums").text.toInt
    val hbaseCoalesceNums = (xml \\ "hbase_coalesce_nums").text.toInt
    
    val mysqlUser = (xml \\ "mysql_user").text
    val mysqlPasswd = (xml \\ "mysql_passwd").text
    val mysqlHost = (xml \\ "mysql_host").text
    val mysqlDb1 = (xml \\ "mysql_db1").text
    val mysqlTb = (xml \\ "mysql_tb_usertype").text
    val mysqlUrl = "jdbc:mysql://" + mysqlHost + ":3306/" + mysqlDb1 //Mysql URL ip_control

    val hbaseRootDir = (xml \\ "hbase_root_dir").text
    val hbaseZkPort = (xml \\ "hbase_zookeeper_port").text
    val hbaseZkQuorum = (xml \\ "hbase_zookeeper_quorum").text
    val resultHbaseTable = (xml \\ "result_hbase_table").text
    val cfName = (xml \\ "column_family_name").text
    val colName1 = (xml \\ "column_name1").text
    val colName2 = (xml \\ "column_name2").text
    
    val kafkaZkQuorum = (xml \\ "kafka_zookeeper_quorum").text //kafka 配置
    val kafkaBootstrapServers = (xml \\ "kafka_bootstrap_servers").text
    val autoOffsetReset = (xml \\ "auto.offset.reset").text // "largest"
    val enableAutoCommit = (xml \\ "enable.auto.commit").text // "false: java.lang.Boolean"
    val consumerGroupId = (xml \\ "consumer_group_id").text
    val kafkaTopic = (xml \\ "kafka_topic").text
    val kafkaTopicSet = Set(kafkaTopic)
    
    val funcFormulas = xml \\ "funcFormula"
    val formula0 = funcFormulas(0).text.r.pattern
    val formula1 = funcFormulas(1).text.r.pattern
    val formula2 = funcFormulas(2).text.r.pattern
    val formula3 = funcFormulas(3).text.r.pattern
    
    val correctResultPath = checkResultPath + "/correct"
    val errorResultPath = checkResultPath + "/error"
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    
    val colFamily = Bytes.toBytes(cfName) //列族
    val colDns = Bytes.toBytes(colName1) //列
    val colDay = Bytes.toBytes(colName2) //列
    
    /*监听kafka更新文件 */
    val SparkConf = new SparkConf().setAppName("DatacheckDW_" + provinceLongID)
    val ssc = new StreamingContext(SparkConf, Seconds(batchDuration))
    val sqlContext = new SQLContext(ssc.sparkContext)

    /*Spark读取用户类型mysql匹配表*/
    val ipCtr = sqlContext.read.format("jdbc").options(Map(
      "url" -> mysqlUrl,
      "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> mysqlTb,
      "user" -> mysqlUser,
      "password" -> mysqlPasswd)).load()
    if (provinceLongID == "1") provinceLongID = "750000"
    val ipCtrTmp = ipCtr.filter("location = " + provinceLongID).select("int_startip", "int_endip", "user_type") //过滤掉不相关字段
    val ipCtrBroadcast = ssc.sparkContext.broadcast(ipCtrTmp.collectAsList()) //广播mysql数据表

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> kafkaBootstrapServers,
      "group.id" -> consumerGroupId,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "enable.auto.commit" -> enableAutoCommit,
      "auto.offset.reset" -> autoOffsetReset)

    // val srcData = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
    //   ssc, kafkaParams, offsets, messageHandler) //kafka存储原始日志
      
    val srcData = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
       ssc, kafkaParams, kafkaTopicSet)
        
    srcData.foreachRDD(rdd => {

      if (!rdd.isEmpty()) {
        val currentTime = dateFormat.format(System.currentTimeMillis())
        
        // val rddNew = rdd.repartition(srcRddRepartitionNums)
        /*数据检核*/
        val pairs = rdd.map(line => (line._2, line._2.split(srcSplitPattern)))
        //val pairs = rdd.map(line => (line, line.split(srcSplitPattern)))
        val checkResult = pairs.map(pair => (pair._1,
          if (pair._2.length >= splitLimitNum &&
            formula0.matcher(pair._2(0)).matches &&
            formula1.matcher(pair._2(1)).matches &&
            formula2.matcher(pair._2(2)).matches &&
            formula3.matcher(pair._2(3)).matches) 1 else 0))

        // checkResult.persist()

        /*生成合格数据并保存*/
        val correctData = checkResult.filter { case (pair, flag) => flag == 1 }.map(x => x._1)
        // correctData.persist()
        // val correctDataColalesce = correctData.coalesce(checkCorrectCoalesceNums, true)
        // correctDataColalesce.saveAsTextFile(correctResultPath + "/" + currentTime)
        // correctData.count()

        /*生成垃圾数据并保存*/
        // val errorData = checkResult.filter { case (pair, flag) => flag == 0 }.map(x => x._1._1)
        // errorData.coalesce(checkErrorCoalesceNums, true).saveAsTextFile(errorResultPath + "/" + currentTime)
        // errorData.count()
        // checkResult.unpersist()
        /*轻度汇总*/
        val DNSLOG_PARSE = correctData.map { str =>
          //有效域名判断、省份代码处理、目的IP处理
          val p = str.split(srcSplitPattern)

          val ipLong = InternalProcess.IPv4ToLong(p(0))
          val validDomain = InternalProcess.ValidDomian(p(1))
          val timeOrigin = p(2)
          val timeMin = InternalProcess.ComputeMin(timeOrigin.substring(10, 12)) + "|" + timeOrigin.substring(0, 10)
          val IPTarget = InternalProcess.GetFirstIP(p(3)) //第一个目的IP
          //获取用户类型
          val ipCtrNew = ipCtrBroadcast.value
          var userType = "1"

          val beforeUserTpyeTime = dateFormat.format(System.currentTimeMillis())
          var found_user_type = false
          for (i <- 1 to ipCtrNew.size() - 1 if !found_user_type) {
            val ipStartTmp = ipCtrNew.get(i).get(0)
            val ipStart = ipStartTmp.toString().toLong
            val ipStopTmp = ipCtrNew.get(i).get(1)
            val ipStop = ipStopTmp.toString().toLong
            if (ipLong != -1 && ipLong >= ipStart && ipLong <= ipStop){
              userType = ipCtrNew.get(i).get(2).toString()
              found_user_type = true
            }
          }
          val afterUserTpyeTime = dateFormat.format(System.currentTimeMillis())
          println("++++++++++++++++" + beforeUserTpyeTime + "++++++++++++++++")
          println(("++++++++++++++++" + afterUserTpyeTime + "++++++++++++++++"))
          //域名、时间、目的IP、用户类型、上传省份
          DNSlog1(validDomain, timeMin, IPTarget, userType, provinceLongID)
          //DNSlog1(p(1), p(2), p(0), p(0), p(0))
        }

        // correctData.unpersist()
        val DNSLOG_F = DNSLOG_PARSE.filter(line => line.USER_TYPE != "-1" && line.DD_NM_VALID != "0") //清除无效记录       
        val DNSLOG_ADD = DNSLOG_F.map(log => (log, 1))
        val DNSLOG_FOLD = DNSLOG_ADD.foldByKey(0)(_ + _)
        val DNSLOG_GRP = DNSLOG_FOLD .reduceByKey(_ + _)
        val DNSLOG_KEYMERGE = DNSLOG_GRP.map(KeyMergeResult)
        // val DNSLOG_KEYMERGE_COALESCE = DNSLOG_KEYMERGE.coalesce(hbaseCoalesceNums, true)
       
        /*HBASE操作*/
        DNSLOG_KEYMERGE.foreachPartition(partitionRecords => {
          val connection = InternalProcess.getHbaseConn(hbaseRootDir, hbaseZkPort, hbaseZkQuorum)
          val hbaseTableName = TableName.valueOf(resultHbaseTable)
          connection.getBufferedMutator(hbaseTableName) //设置批量更新、关闭自动
          val table = connection.getTable(hbaseTableName) //获取表连接

          partitionRecords.foreach(s => {
            var count = s.COUNT
            val hashKey = MD5Hash.getMD5AsHex(Bytes.toBytes(s.KEY.toString())) //hash Row Key
            val key = hashKey.substring(0, 8) + "|" + s.KEY.toString()

            //根据KEY读取HBASE表,更新(五分钟级)汇总次数 
            val g = new Get(Bytes.toBytes(key))
            g.addColumn(colFamily, colDns)
            val res = table.get(g)
            if (!res.isEmpty()) {
              val value = Bytes.toString(res.getValue(colFamily, colDns))
              count = value.toLong + count
            }

            //写入HBASE      
            val put = new Put(Bytes.toBytes(key))
            put.addColumn(colFamily, colDns, Bytes.toBytes(count.toString())) //每五分钟的统计值
            put.addColumn(colFamily, colDay, Bytes.toBytes(s.DAY)) //加入day列，方便后面判断写入 hive
            put.setDurability(Durability.SKIP_WAL) //不写WAL日志
            table.put(put)
          })
          table.close()
          connection.close()
        })
      }
      //更新offsets
      // RecoverableDirectKafka.updateZKOffsets(offsetsList, zkServers, kafka_group_id, kafka_topic)
    })

    ssc.start() //开始接收数据
    ssc.awaitTermination() //等待处理停止
  }
}
