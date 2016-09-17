/*
 *    Copyright (C) 2015 Mesosphere, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.victorursan.recordio.scala.com.victorursan.recordio

import java.io.InputStream
import java.util.Arrays._

import com.victorursan.recordio.{RecordIOOperator, RecordIOSubscriber, RecordIOUtils}
import io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.mesos.v1.scheduler.Protos.Event
import org.junit.Assert._
import rx.Subscriber
import rx.observers.TestSubscriber


object RecordIOOperatorTest {
  private val EVENT_PROTOS: List[Event] = List(TestingProtos.SUBSCRIBED, TestingProtos.HEARTBEAT, TestingProtos.HEARTBEAT, TestingProtos.HEARTBEAT,
    TestingProtos.HEARTBEAT, TestingProtos.HEARTBEAT, TestingProtos.HEARTBEAT, TestingProtos.HEARTBEAT, TestingProtos.HEARTBEAT,
    TestingProtos.HEARTBEAT, TestingProtos.HEARTBEAT)
  private val EVENT_CHUNKS: List[Array[Byte]] = EVENT_PROTOS.map{ (p: Event) => p.getMessage.toByteArray}
                                                                  .map{(arr: Array[Byte]) => RecordIOUtils.createChunk(arr)}

  private[recordio] def runTestOnChunks(chunks: List[ByteBuf]): List[Event] = {
    val child: TestSubscriber[Array[Byte]] = new TestSubscriber[Array[Byte]]
    val call: Subscriber[_ >: ByteBuf] = new RecordIOOperator().call(child)
    assertTrue(call.isInstanceOf[RecordIOSubscriber])
    val subscriber: RecordIOSubscriber = call.asInstanceOf[RecordIOSubscriber]
    chunks.foreach(subscriber.onNext)
    child.assertNoErrors()
    child.assertNotCompleted()
    child.assertNoTerminalEvent()
    assertTrue(subscriber.messageSizeBytesBuffer.isEmpty)
    assertTrue(subscriber.messageBytes.isEmpty)
    assertTrue(subscriber.remainingBytesForMessage == 0)
    //    CollectionUtils.listMap(child.getOnNextEvents, bs -> {
    //        return Event.parseFrom(bs);
    //    })
//    var toReturn: List[Event] = List[Event]()
//    child.getOnNextEvents.stream().map[Event]{ arr: Array[Byte]  =>{
//      val smth = new ByteArrayInputStream(arr)
////      smth.read(arr)
//      Event.parseFrom(smth)
//    }
//                                             }.
//    toReturn
    List[Event]()
  }

  private[recordio] def partitionIntoArraysOfSize(allBytes: Array[Byte], chunkSize: Int): List[Array[Byte]] = {
    var chunks: List[Array[Byte]] = List[Array[Byte]]()
    val numFullChunks: Int = allBytes.length / chunkSize
    var chunkStart: Int = 0
    var chunkEnd: Int = 0
    var i: Int = 0
    for (_ <- 0 to numFullChunks) {
        chunkEnd += chunkSize
        chunks = chunks.:+(copyOfRange(allBytes, chunkStart, chunkEnd))
        chunkStart = chunkEnd
      }
    if (chunkStart < allBytes.length) {
      chunks = chunks.:+(copyOfRange(allBytes, chunkStart, allBytes.length))
    }
    chunks
  }

  private[recordio] def concatAllChunks(chunks: List[Array[Byte]]): Array[Byte] = chunks.flatten.toArray

//  @Test
//  @throws[Exception]
  def correctlyAbleToReadEventsFromEventsBinFile() {
    val inputStream: InputStream = this.getClass.getResourceAsStream("/events.bin")
    var chunks: List[ByteBuf] = List[ByteBuf]()
    val bytes: Array[Byte] = new Array[Byte](100)
    var read: Int = inputStream.read(bytes)
    while (read  != -1) {
      chunks = chunks.:+(Unpooled.copiedBuffer(bytes, 0, read))
      read = inputStream.read(bytes)
    }
    val events: List[Event] = RecordIOOperatorTest.runTestOnChunks(chunks)
    val eventTypes: List[Event.Type] = events.map(_.getType)
    assertTrue(eventTypes.equals(List(Event.Type.SUBSCRIBED, Event.Type.HEARTBEAT, Event.Type.OFFERS, Event.Type.OFFERS,
      Event.Type.OFFERS, Event.Type.HEARTBEAT, Event.Type.OFFERS, Event.Type.OFFERS, Event.Type.OFFERS, Event.Type.HEARTBEAT,
      Event.Type.OFFERS, Event.Type.OFFERS, Event.Type.HEARTBEAT, Event.Type.OFFERS, Event.Type.OFFERS, Event.Type.HEARTBEAT,
      Event.Type.OFFERS, Event.Type.OFFERS, Event.Type.OFFERS, Event.Type.HEARTBEAT, Event.Type.OFFERS, Event.Type.OFFERS,
      Event.Type.HEARTBEAT, Event.Type.OFFERS, Event.Type.OFFERS, Event.Type.OFFERS, Event.Type.HEARTBEAT, Event.Type.OFFERS,
      Event.Type.OFFERS, Event.Type.OFFERS, Event.Type.HEARTBEAT, Event.Type.OFFERS, Event.Type.OFFERS, Event.Type.HEARTBEAT,
      Event.Type.OFFERS, Event.Type.OFFERS, Event.Type.OFFERS, Event.Type.HEARTBEAT, Event.Type.OFFERS, Event.Type.HEARTBEAT,
      Event.Type.HEARTBEAT, Event.Type.HEARTBEAT)))
  }
//
//  @Test
//  @throws[Exception]
  def readEvents_eventsNotSpanningMultipleChunks() {
    val eventBufs: List[ByteBuf] =  RecordIOOperatorTest.EVENT_CHUNKS.map(Unpooled.copiedBuffer)
    val events: List[Event] = RecordIOOperatorTest.runTestOnChunks(eventBufs)
    assertTrue(events.equals(RecordIOOperatorTest.EVENT_PROTOS))
  }

//  @Test
//  @throws[Exception]
  def readEvents_eventsSpanningMultipleChunks() {
    val allBytes: Array[Byte] = RecordIOOperatorTest.concatAllChunks(RecordIOOperatorTest.EVENT_CHUNKS)
    val arrayChunks: List[Array[Byte]] = RecordIOOperatorTest.partitionIntoArraysOfSize(allBytes, 10)
    val bufChunks: List[ByteBuf] = arrayChunks.map(Unpooled.copiedBuffer)
    val events: List[Event] = RecordIOOperatorTest.runTestOnChunks(bufChunks)
    assertTrue(events.equals(RecordIOOperatorTest.EVENT_PROTOS))
  }
//
//  @Test
//  @throws[Exception]
  def readEvents_multipleEventsInOneChunk() {
    val subHbOffer: List[Event] = List(TestingProtos.SUBSCRIBED, TestingProtos.HEARTBEAT, TestingProtos.OFFER)
    val eventChunks: List[Array[Byte]] = subHbOffer.map((e : Event) => RecordIOUtils.createChunk(e.toByteArray))
    val singleChunk: List[ByteBuf] = List(Unpooled.copiedBuffer(RecordIOOperatorTest.concatAllChunks(eventChunks)))
    val events: List[Event] = RecordIOOperatorTest.runTestOnChunks(singleChunk)
    assertTrue(events.equals(subHbOffer))
  }
}
