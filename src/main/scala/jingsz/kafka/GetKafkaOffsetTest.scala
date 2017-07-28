package jingsz.kafka

import java.io.IOException

import kafka.api._
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.javaapi.OffsetFetchResponse
import kafka.network.BlockingChannel


object OffsetFetchTest {
  def main(args: Array[String]): Unit = {
    try {
      val channel = new BlockingChannel("ubuntu", 9092,
        BlockingChannel.UseDefaultBufferSize,
        BlockingChannel.UseDefaultBufferSize,
        5000 /* read timeout in millis */)
      channel.connect()
      val MY_GROUP = "test_jingsz"
      val correlationId = 0
      val testPartition0 = TopicAndPartition("test", 0)

      val partitions = List(testPartition0)
      val fetchRequest = OffsetFetchRequest(
        MY_GROUP,
        partitions,
        1 /* version */ , // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
        correlationId
      )

      try {
        channel.send(fetchRequest)
        val fetchResponse = OffsetFetchResponse.readFrom(channel.receive().payload())
        val result = fetchResponse.offsets.get(testPartition0)
        val offsetFetchErrorCode = result.error
        if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode) {
          channel.disconnect()
          // Go to step 1 and retry the offset fetch
        } else if (offsetFetchErrorCode == ErrorMapping.OffsetsLoadInProgressCode) {
          // retry the offset fetch (after backoff)
        } else {
          val retrievedOffset = result.offset
          val retrievedMetadata = result.metadata
          println(retrievedOffset, retrievedMetadata)
        }
      }
      catch {
        case ex: IOException => {
          channel.disconnect()
          // Go to step 1 and then retry offset fetch after backoff
          println("IO Exception")
        }
      }
    }
  }
}
