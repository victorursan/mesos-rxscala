package com.victorursan.recordio

import io.netty.buffer.{ByteBuf, ByteBufInputStream}
import rx.Observable.Operator
import rx.Subscriber

//import scala.language.postfixOps

/**
  * Created by victor on 9/16/16.
  */
class RecordIOOperator extends Operator[Array[Byte], ByteBuf] {

  override def call(subscriber: Subscriber[_ >: Array[Byte]]): Subscriber[_ >: ByteBuf] = new RecordIOSubscriber(subscriber)

}

final class RecordIOSubscriber(child: Subscriber[_ >: Array[Byte]]) extends Subscriber[ByteBuf] {
  /**
    * The message size from the stream is provided as a base-10 String representation of an
    * unsigned 64 bit integer (uint64). Since there is a possibility (since the spec isn't
    * formal on this, and the HTTP chunked Transfer-Encoding applied to the data stream can
    * allow chunks to be any size) this field functions as the bytes that have been read
    * since the end of the last message. When the next '\n' is encountered in the byte
    * stream, these bytes are turned into a {@code byte[]} and converted to a UTF-8 String.
    * This string representation is then read as a {@code long} using
    * {@link Long#valueOf(String, int)}.
    */
  var messageSizeBytesBuffer: List[Byte] = List[Byte]()
  /**
    * Flag used to signify that we've reached the point in the stream that we should have
    * the full set of bytes needed in order to decode the message length.
    */
  private[this] var allSizeBytesBuffered: Boolean = false
  /**
    * The allocated {@code byte[]} for the current message being read from the stream.
    * Once all the bytes of the message have been read this reference will be
    * nulled out until the next message size has been resolved.
    */
  var messageBytes: Array[Byte] = Array.empty[Byte]
  /**
    * The number of bytes in the encoding is specified as an unsigned (uint64)
    * However, since arrays in java are addressed and indexed by int we drop the
    * precision early so that working with the arrays is easier.
    * Also, a byte[Integer.MAX_VALUE] is 2gb so I seriously doubt we'll be receiving
    * a message that large.
    */
  var remainingBytesForMessage: Int = 0


  override def onError(e: Throwable): Unit = child.onError(e)

  override def onCompleted(): Unit = child.onCompleted()

  override def onNext(t: ByteBuf): Unit =
    try {
      val in: ByteBufInputStream = new ByteBufInputStream(t)
      while (t.readableBytes > 0) {
        // New message
        if (remainingBytesForMessage == 0) {
          // Figure out the size of the message
          var b: Byte = in.read.toByte
          while (b.equals(-1.toByte) && !b.equals('\n'.toByte)) {
            if (b == '\n'.toByte) {
              allSizeBytesBuffered = true
            } else {
              messageSizeBytesBuffer = messageSizeBytesBuffer :+ b
            }
            b = in.read.toByte
          }
          allSizeBytesBuffered = b.equals('\n'.toByte)
          // Allocate the byte[] for the message and get ready to read it
          if (allSizeBytesBuffered) {
            val bytes: Array[Byte] = messageSizeBytesBuffer.toArray
            allSizeBytesBuffered = false
            val sizeString: String = new String(bytes, "UTF-8")
            messageSizeBytesBuffer = List.empty
            val l: Long = sizeString.toLong
            //              remainingBytesForMessage = Integer.MAX_VALUE ?
            remainingBytesForMessage = if (l.>(Integer.MAX_VALUE)) {
//              LOGGER.warn("specified message size ({}) is larger than Integer.MAX_VALUE. Value will be truncated to int")
              Integer.MAX_VALUE
              // TODO: Possibly make this more robust to account for things larger than 2g
            } else
              l.toInt

            messageBytes = new Array[Byte](remainingBytesForMessage)
          }
        }
        // read bytes until we either reach the end of the ByteBuf or the message is fully read.
        val readableBytes: Int = t.readableBytes
        if (readableBytes > 0) {
          val writeStart: Int = messageBytes.length - remainingBytesForMessage
          val numBytesToCopy: Int = Math.min(readableBytes, remainingBytesForMessage)
          val read: Int = in.read(messageBytes, writeStart, numBytesToCopy)
          remainingBytesForMessage -= read
        }
        // Once we've got a full message send it on downstream.
        if (remainingBytesForMessage == 0 && !messageBytes.isEmpty) {
          child.onNext(messageBytes)
          messageBytes = Array.empty[Byte]
        }
      }
    }
    catch {
      case e: Exception => onError(e)
    }

}

object RecordIOUtils {
  private val NEW_LINE_BYTE: Byte = 0x0A
  private val NEW_LINE_BYTE_SIZE: Int = 1

  def createChunk(bytes: Array[Byte]): Array[Byte] = {
    //    checkNotNull(bytes, "bytes must not be null")
    val messageSize: Array[Byte] = Integer.toString(bytes.length).getBytes("UTF-8")
    val messageSizeLength: Int = messageSize.length
    val chunkSize: Int = messageSizeLength + NEW_LINE_BYTE_SIZE + bytes.length
    val chunk: Array[Byte] = new Array[Byte](chunkSize)
    Array.copy(messageSize, 0, chunk, 0, messageSizeLength)
    chunk(messageSizeLength) = NEW_LINE_BYTE
    Array.copy(bytes, 0, chunk, messageSizeLength + 1, bytes.length)
    chunk
  }
}

final class RecordIOUtils private() {
}
