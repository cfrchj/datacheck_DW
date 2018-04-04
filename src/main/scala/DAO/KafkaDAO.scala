package DAO

import Model.ConfPara
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder

object KafkaDAO {
  def directReadDataFromKafka(cp : ConfPara, ssc : StreamingContext): InputDStream[(String, String)] ={
    val kafkaParams : Map[String, String] = Map[String, String](
      "bootstrap.servers" -> cp.kafkaBootstrapServers,
      "group.id" -> cp.consumerGroupId,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "enable.auto.commit" -> cp.enableAutoCommit,
      "auto.offset.reset" -> cp.autoOffsetReset)
    val srcData: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder ](
      ssc, kafkaParams, cp.kafkaTopicSet
    )
    srcData
  }
}
