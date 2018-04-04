package Model

import java.util.regex.Pattern

import scala.xml.{NodeSeq, XML}

class ConfPara(confPath : String) extends Serializable {

  val xml = XML.loadFile(confPath)
  val srcSplitPattern : String = "\\" + (xml \\ "src_split_pattern").text
  val splitLimitNum : Int = (xml \\ "split_limit_num").text.toInt
  val batchDuration : Long = (xml \\ "batch_duration").text.toLong
  var provinceLongID : String = (xml \\ "prov_long_id").text
  val checkResultPath : String = (xml \\ "check_result_path").text
  val srcRddRepartitionNums : Int = (xml \\ "src_rdd_repartition_nums").text.toInt
  val checkCorrectCoalesceNums : Int = (xml \\ "check_correct_coalesce_nums").text.toInt
  val checkErrorCoalesceNums : Int= (xml \\ "check_error_coalesce_nums").text.toInt
  val hbaseCoalesceNums : Int= (xml \\ "hbase_coalesce_nums").text.toInt

  val mysqlUser : String = (xml \\ "mysql_user").text
  val mysqlPasswd : String = (xml \\ "mysql_passwd").text
  val mysqlHost : String = (xml \\ "mysql_host").text
  val mysqlDb1 : String = (xml \\ "mysql_db1").text
  val mysqlTb : String = (xml \\ "mysql_tb_usertype").text
  val mysqlUrl : String = "jdbc:mysql://" + mysqlHost + ":3306/" + mysqlDb1

  val hbaseRootDir : String = (xml \\ "hbase_root_dir").text
  val hbaseZkPort : String = (xml \\ "hbase_zookeeper_port").text
  val hbaseZkQuorum : String = (xml \\ "hbase_zookeeper_quorum").text
  val resultHbaseTable : String = (xml \\ "result_hbase_table").text
  val cfName : String = (xml \\ "column_family_name").text
  val colName1 : String = (xml \\ "column_name1").text
  val colName2 : String = (xml \\ "column_name2").text

  val kafkaZkQuorum : String = (xml \\ "kafka_zookeeper_quorum").text //kafka 配置
  val kafkaBootstrapServers : String = (xml \\ "kafka_bootstrap_servers").text
  val autoOffsetReset : String = (xml \\ "auto.offset.reset").text // "largest"
  val enableAutoCommit : String = (xml \\ "enable.auto.commit").text // "false: java.lang.Boolean"
  val consumerGroupId : String = (xml \\ "consumer_group_id").text
  val kafkaTopic : String = (xml \\ "kafka_topic").text
  val kafkaTopicSet = Set(kafkaTopic)

  //val funcFormulas : NodeSeq = xml \\ "funcFormula"
  val formula0 : Pattern = (xml \\ "funcFormula")(0).text.r.pattern
  val formula1 : Pattern = (xml \\ "funcFormula")(1).text.r.pattern
  val formula2 : Pattern = (xml \\ "funcFormula")(2).text.r.pattern
  val formula3 : Pattern = (xml \\ "funcFormula")(3).text.r.pattern
}
