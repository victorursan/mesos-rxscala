package com.victorursan.recordio.scala.com.victorursan.recordio

import com.victorursan.recordio.RecordIOUtils
import io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.mesos.v1.scheduler.Protos.Event

/**
  * Created by victor on 9/17/16.
  */

class RecordIOOperatorChunkSizesTest {


  val chunkSizes: List[Int] = List(1, 2, 3, 4, 5, 10, 20, 30, 40, 50, 60)

  def test(chunkSize: Int = 1) {
    val chunk: Array[Byte] = RecordIOUtils.createChunk(TestingProtos.SUBSCRIBED.toByteArray)
    val bytes: List[Array[Byte]] = RecordIOOperatorTest.partitionIntoArraysOfSize(chunk, chunkSize)
    val chunks: List[ByteBuf] = bytes.map(Unpooled.copiedBuffer)
    val events: List[Event] = RecordIOOperatorTest.runTestOnChunks(chunks)
    val eventTypes: List[Event.Type] = events.map { (e: Event) => e.getType }
    eventTypes.equals(List(Event.Type.SUBSCRIBED))
  }

  def toRunmain(): Unit = {
    for (i <- chunkSizes) {
      test(i)
    }

  }
}