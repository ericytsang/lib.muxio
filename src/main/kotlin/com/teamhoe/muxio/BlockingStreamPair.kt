package com.teamhoe.muxio

import java.io.InputStream
import java.io.OutputStream
import java.io.DataInputStream
import java.nio.ByteBuffer

import java.util.concurrent.Semaphore

/**
 * Created by Eric Tsang on 12/14/2015.
 */
class BlockingStreamPair private constructor(
    private val multiplexedInputStream:DataInputStream,
    private val multiplexedOutputStream:OutputStream)
{
    companion object
    {
        fun wrap(inputStream:InputStream,outputStream:OutputStream):BlockingStreamPair =
            BlockingStreamPair(DataInputStream(inputStream),outputStream)
    }

    val inputStream:InputStream = BlockingQueueInputStream()
    val outputStream:OutputStream = UserOutputStream()

    private val USER_DATA:Short = 0
    private val PROTOCOL_DATA:Short = 1

    private val userInputStream = inputStream as BlockingQueueInputStream
    private val protocolOutputStream = ProtocolOutputStream()

    private val releasedOnAck = Semaphore(0)

    init
    {
        ReadThread().start()
    }

    private fun receiveData()
    {
        // read the data from the multiplexedInputStream
        val len = multiplexedInputStream.readInt()
        val data = ByteArray(len)
        multiplexedInputStream.read(data)

        // acknowledge that we have received the data
        protocolOutputStream.write(0)

        // write the data to the user input stream so they can read it
        userInputStream.sourceQueue.put(data)
    }

    private fun receiveAck()
    {
        // read the data from the multiplexedInputStream
        val len = multiplexedInputStream.readInt()
        val data = ByteArray(len)
        multiplexedInputStream.read(data)

        // handle the ack
        releasedOnAck.release()
    }

    // user write operations are headed with USER_DATA
    private inner class UserOutputStream:MultiplexingOutputStream(multiplexedOutputStream)
    {
        override fun write(b:ByteArray,off:Int,len:Int)
        {
            synchronized(this)
            {
                // write the data out the stream
                super.write(b,off,len)

                // block until we get the ack for the data we sent
                releasedOnAck.acquireUninterruptibly()
            }
        }

        override fun makeHeader(b:ByteArray,off:Int,len:Int):ByteArray
        {
            return ByteBuffer.allocate(6)
                .putShort(USER_DATA)
                .putInt(len)
                .array()
        }
    }

    // protocol write operations are headed with PROTOCOL_DATA
    private inner class ProtocolOutputStream:MultiplexingOutputStream(multiplexedOutputStream)
    {
        override fun makeHeader(b:ByteArray,off:Int,len:Int):ByteArray
        {
            return ByteBuffer.allocate(6)
                .putShort(PROTOCOL_DATA)
                .putInt(len)
                .array()
        }
    }

    // reads from underlying input stream; put data into public input stream,
    // and handle received acks
    private inner class ReadThread:Thread()
    {
        override fun run()
        {
            while(true)
            {
                try
                {
                    // read header from stream
                    val type = multiplexedInputStream.readShort()

                    // parse data depending on header
                    when (type)
                    {
                        USER_DATA -> receiveData()
                        PROTOCOL_DATA -> receiveAck()
                        else -> throw RuntimeException("unknown type: $type")
                    }
                }
                catch(ex:InterruptedException)
                {
                    return
                }
            }
        }
    }
}
