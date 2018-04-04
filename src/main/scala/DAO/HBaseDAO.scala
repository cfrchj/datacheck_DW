package DAO

import Model.ConfPara
import cn.ac.iie.iaa.SparkDatacheckDW.DNSlog2
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.spark.rdd.RDD

object HBaseDAO {
  /*get a HBase connection*/
  def getHbaseConn(rootDir: String, clientPort: String, quorum: String): Connection = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.rootdir", rootDir)
    conf.set("hbase.zookeeper.property.clientPort", clientPort) //集群zookeeper端口
    conf.set("hbase.zookeeper.quorum", quorum) //zookeeper节点
    val connection = ConnectionFactory.createConnection(conf) //获取hbase连接
    connection
  }

  def writeRDDToHBase(rdd : RDD[DNSlog2], cp : ConfPara) : Unit={
      val colFamily = Bytes.toBytes(cp.cfName) //列族
      val colDns = Bytes.toBytes(cp.colName1) //列
      val colDay = Bytes.toBytes(cp.colName2) //列
      rdd.foreachPartition(partitionRecords => {
      val hbaseTableName = TableName.valueOf(cp.resultHbaseTable)
      val connection = getHbaseConn(cp.hbaseRootDir, cp.hbaseZkPort, cp.hbaseZkQuorum)
      connection.getBufferedMutator(hbaseTableName) //设置批量更新、关闭自动
      val table = connection.getTable(hbaseTableName) //获取表连接

      partitionRecords.foreach(s => {
        var count: Long = s.COUNT
        val hashKey: String = MD5Hash.getMD5AsHex(Bytes.toBytes(s.KEY.toString())) //hash Row Key
        val key: String = hashKey.substring(0, 8) + "|" + s.KEY.toString()
        val g: Get = new Get(Bytes.toBytes(key))
        g.addColumn(colFamily, colDns)
        val res = table.get(g)
        if (!res.isEmpty()) {
          val value = Bytes.toString(res.getValue(colFamily, colDns))
          count = value.toLong + count
        }
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
}
