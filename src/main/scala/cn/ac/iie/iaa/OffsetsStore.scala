package cn.ac.iie.iaa

import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.kafka.OffsetRange

import kafka.api.OffsetRequest
import kafka.api.PartitionOffsetRequestInfo
import kafka.common.BrokerNotAvailableException
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.ZKGroupTopicDirs
import kafka.utils.ZKStringSerializer
import kafka.utils.ZkUtils

/**
 * Created by wangwei01 on 2016/11/18.
 *
 * 1. 程序创建StreamingContext时，如果存在checkpoint目录，则直接根据checkpoint目录中的数据恢复一个StreamingContext
 * 如果不存在，则会从zookeeper中读取当前consumer groupId消费的offset，然后再从kafka中读取到该topic下最早的的offset
 * 如果从zookeeper中读取到的offset为空或者小于从kafka中读取到该topic下最早的offset，则采用从kafka中读取的offset
 * 否则就用zookeeper中存储的offset
 * 2. 每次生成KafkaRDD之后，都会将它对应的OffsetRange中的untilOffset存入到zookeeper
 *
 */

object RecoverableDirectKafka {

  def getOffsets(zkServers: String, groupId: String, topics: String): Map[TopicAndPartition, Long] = {
    //  val zkClient = new ZkClient(zkServers) 
    // 必须要使用带有ZkSerializer参数的构造函数来构造，否则在之后使用ZkUtils的一些方法时会出错，而且在向zookeeper中写入数据时会导致乱码 
    // org.I0Itec.zkclient.exception.ZkMarshallingError: java.io.StreamCorruptedException: invalid stream header: 7B227665 
    val zkClient = new ZkClient(zkServers, Integer.MAX_VALUE, 60000, ZKStringSerializer)
    val topicPartitions = ZkUtils.getPartitionsForTopics(zkClient, topics.split(",")).head

    val topic = topicPartitions._1
    val partitions = topicPartitions._2

    val topicDirs = new ZKGroupTopicDirs(groupId, topic)
    var offsetsMap: Map[TopicAndPartition, Long] = Map()

    partitions.foreach { partition =>
      val zkPath = s"${topicDirs.consumerOffsetDir}/$partition" // /consumers/[groupId]/offsets/[topic]/partition 
      ZkUtils.makeSurePersistentPathExists(zkClient, zkPath) // 如果zookeeper之前不存在该目录，就直接创建 

      val tp = TopicAndPartition(topic, partition)
      // 得到kafka中该partition的最早时间的offset 
      val offsetForKafka = getOffsetFromKafka(zkServers, tp, OffsetRequest.EarliestTime)

      // 得到zookeeper中存储的该partition的offset 
      val offsetForZk = ZkUtils.readDataMaybeNull(zkClient, zkPath) match {
        case (Some(offset), stat) =>
          Some(offset)
        case (None, stat) => // zookzeeper中未存储偏移量 
          None
      }

      var offsetLong : Long = 0
      if (offsetForZk.get != null){
        offsetLong = offsetForZk.get.toLong
      }
        
      if (offsetForZk.isEmpty ||  offsetLong < offsetForKafka) { // 如果zookzeeper中未存储偏移量或zookzeeper中存储的偏移量已经过期 
        println("Zookeeper don't save offset or offset has expire!")
        offsetsMap += (tp -> offsetForKafka)
      } else {
        offsetsMap += (tp -> offsetLong)
      }
    }
    println(s"offsets: $offsetsMap")
    zkClient.close()
    offsetsMap
  }

  private def getOffsetFromKafka(zkServers: String, tp: TopicAndPartition, time: Long): Long = {
    val zkClient = new ZkClient(zkServers, Integer.MAX_VALUE, 60000, ZKStringSerializer)
    val request = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(time, 1)))
    var head :Long = 0

    //  得到每个分区的leader（某个broker） 
    ZkUtils.getLeaderForPartition(zkClient, tp.topic, tp.partition) match {
      case Some(brokerId) =>
        // 根据brokerId得到leader这个broker的详细信息 
        ZkUtils.getBrokerInfo(zkClient, brokerId) match {
          case Some(broker) =>
            // 使用leader的host和port来创建一个SimpleConsumer 
            val consumer = new SimpleConsumer(broker.host, broker.port, 10000, 100000, "getOffsetShell")
            val offsets = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(tp).offsets
            head = offsets.head
          case None =>
            throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
        }
      case None =>
        throw new Exception("No broker for partition %s - %s".format(tp.topic, tp.partition))
    }
    zkClient.close()
    head
  }

  def updateZKOffsets(offsetRanges: Array[OffsetRange],zkServers: String, groupId: String, topics: String): Unit = {
    val zkClient = new ZkClient(zkServers, Integer.MAX_VALUE, 60000, ZKStringSerializer)
    val topicDirs = new ZKGroupTopicDirs(groupId, topics)

    for (offset <- offsetRanges) {
      val zkPath = s"${topicDirs.consumerOffsetDir}/${offset.partition}"// /consumers/[groupId]/offsets/[topic]/partition 
      ZkUtils.updatePersistentPath(zkClient, zkPath, offset.untilOffset.toString)
      println(s"[${offset.topic},${offset.partition}]: ${offset.fromOffset},${offset.untilOffset}") 
    }
    zkClient.close()
  }
} 
 
 
