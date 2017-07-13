package jingsz

import kafka.utils.ZkUtils
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import kafka.common.{KafkaException, NoEpochForPartitionException, TopicAndPartition}


object GetKafkaPartition {

  def getTopicPartitionCount(zkServers: String, topic: String): Int = {
    val client = new ZkClient(zkServers)
    val connection = new ZkConnection(zkServers)
    val utils = new ZkUtils(client, connection, false)
    val partitions = utils.getAllPartitions()
    partitions.foreach((x: TopicAndPartition) => println(x.toString))

    val partitionCount = partitions
      .count(topicPartitionPair => topicPartitionPair.topic == topic)

    client.close
    partitionCount
  }

  def main(args: Array[String]): Unit = {
    println(getTopicPartitionCount("ubuntu:2181", "test"))
  }


}
