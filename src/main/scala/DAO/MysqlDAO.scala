package DAO
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util

import Model.ConfPara
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}
import org.apache.spark.streaming.StreamingContext
object MysqlDAO {
  def getIPControlTable(cp: ConfPara, provinceLongID : String, ssc : StreamingContext): Array[(Long, Long, String)] = {
    val sqlContext = new SQLContext(ssc.sparkContext)
    val ipCtr = sqlContext.read.format("jdbc").options(Map(
      "url" -> cp.mysqlUrl,
      "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> cp.mysqlTb,
      "user" -> cp.mysqlUser,
      "password" -> cp.mysqlPasswd)).load()
    val ipCtrTmp : DataFrame = ipCtr.filter("location = " + provinceLongID).select("int_startip", "int_endip", "user_type") //取出对应省份的起止地址已经用户类型字段
    val ipCtrTmp2: Array[Row] = ipCtrTmp.collect()
    //val sortedIPCtrTable: DataFrame = ipCtrTmp.sort("int_startip")
    val realBroadCast: Array[(Long, Long, String)] = ipCtrTmp2.map { x => (x.get(0).toString().toLong, x.get(1).toString().toLong, x.get(2).toString()) }.sorted
    realBroadCast
  }
}
