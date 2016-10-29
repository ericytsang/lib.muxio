package com.github.ericytsang.lib.muxio

import java.io.DataInputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.LinkedHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * that receives and de-multiplexes data from the [inputStream], and multiplexes
 * data to send out the [outputStream]. provides methods ([connect] and
 * [accept]) which can be used to create an [InputStream] to read de-multiplexed
 * data from the [Multiplexer]'s [inputStream], and an [OutputStream] which can
 * be used to write multiplexed data out the [Multiplexer]'s [outputStream].
 *
 * Created by Eric Tsang on 12/12/2015.
 */
class Demultiplexer(val inputStream:InputStream)
{
    /**
     * underlying [DataInputStream] that data is read and de-multiplexed from.
     */
    private val multiplexedInputStream = DataInputStream(inputStream)

        get()
        {
            if (!multiplexedInputStreamAccess.isHeldByCurrentThread)
            {
                throw IllegalStateException("$multiplexedInputStreamAccess must be acquired before accessing $multiplexedInputStream")
            }
            return field
        }

    /**
     * lock that threads must hold when accessing the [multiplexedInputStream].
     */
    private val multiplexedInputStreamAccess = ReentrantLock()

    /**
     * maps remote source port numbers to the associated [InputStream]s.
     */
    private val demultiplexedInputStreams = LinkedHashMap<Long,MyInputStream>()

    /**
     * queue of input streams waiting to be accepted. they may already contain
     * data ready to be read, or even closed.
     */
    private val pendingInputStreams = LinkedBlockingQueue<AbstractInputStream>()

    /**
     * released whenever a packet is read. a new latch is created
     */
    private var releasedWhenPacketIsRead = CountDownLatch(1)

    fun accept():InputStream
    {
        while (true)
        {
            // try to take the next input stream if it exists
            synchronized(pendingInputStreams)
            {
                if (pendingInputStreams.peek() != null)
                {
                    return pendingInputStreams.take()
                }
            }

            // wait for the next packet to arrive and be processed
            awaitPacketArrivalAndProcessing()
        }
    }

    private fun receiveConnect() = synchronized(demultiplexedInputStreams)
    {
        val port = multiplexedInputStream.readLong()
        if (!demultiplexedInputStreams.containsKey(port))
        {
            val inputStream = MyInputStream()
            demultiplexedInputStreams.put(port,inputStream)
            pendingInputStreams.put(inputStream)
        }
        else
        {
            throw IllegalStateException("previous input stream receiving from port $port was not closed yet")
        }
    }

    private fun receiveData() = synchronized(demultiplexedInputStreams)
    {
        val port = multiplexedInputStream.readLong()
        val len = multiplexedInputStream.readInt()
        val data = ByteArray(len)
        multiplexedInputStream.readFully(data)
        val streamPair = demultiplexedInputStreams[port] ?: throw IllegalStateException("no stream found for port: $port")
        streamPair.sourceQueue.put(data)
    }

    /**
     * unregister and close the indicated input stream.
     */
    private fun receiveCloseRemote() = synchronized(demultiplexedInputStreams)
    {
        val port = multiplexedInputStream.readLong()
        demultiplexedInputStreams.remove(port)?.isEof = true
    }

    /**
     * reads one packet of multiplexed data from the [Multiplexer]'s
     * [inputStream], then parses and multiplexes the received data to the
     * appropriate [InputStream] stored in [demultiplexedInputStreams].
     */
    private fun readPacket()
    {
        // read header from stream
        val type = MessageType.values()[multiplexedInputStream.readShort().toInt()]

        // parse data depending on header
        when (type)
        {
            MessageType.CONNECT -> receiveConnect()
            MessageType.DATA -> receiveData()
            MessageType.CLOSE -> receiveCloseRemote()
        }
    }

    private fun awaitPacketArrivalAndProcessing()
    {
        // acquire the muxed input stream then read and handle one packet
        if (multiplexedInputStreamAccess.tryLock())
        {
            readPacket()
            val releasedWhenPacketIsRead = releasedWhenPacketIsRead
            this.releasedWhenPacketIsRead = CountDownLatch(1)
            multiplexedInputStreamAccess.unlock()
            releasedWhenPacketIsRead.countDown()
        }

        // if we fail to acquire the input stream, just wait until we're
        // notified then repeat the whole process
        else
        {
            releasedWhenPacketIsRead.await()
        }
    }

    private inner class MyInputStream:AbstractInputStream()
    {
        val sourceQueue = LinkedBlockingQueue<ByteArray>()

        var isEof = false

        private var currentData = ByteBuffer.wrap(ByteArray(0))

        override fun doRead(b:ByteArray,off:Int,len:Int):Int
        {
            while (true)
            {
                // make sure there is data in the currentData to consume
                // blocking if needed, else return -1 indicating EOF.
                if (!currentData.hasRemaining())
                {
                    // take the next ByteArray as the currentData to read from
                    // if there could potentially be or is a next ByteArray.
                    if (sourceQueue.isEmpty() && !isEof)
                    {
                        awaitPacketArrivalAndProcessing()
                        continue
                    }
                    else if (sourceQueue.isEmpty() && isEof)
                    {
                        return -1
                    }
                    else if (sourceQueue.isNotEmpty())
                    {
                        currentData = ByteBuffer.wrap(sourceQueue.poll()!!)
                    }
                }

                // read all remaining data into user buffer, or just until the
                // user's bytes to read requirement is met
                val bytesToRead = Math.min(len,currentData.remaining())
                currentData.get(b,off,bytesToRead)

                // return the number of bytes read
                return bytesToRead
            }
        }

        override fun doAvailable():Int
        {
            if (!currentData.hasRemaining() && sourceQueue.isNotEmpty())
            {
                currentData = ByteBuffer.wrap(sourceQueue.poll()!!)
            }

            return currentData.remaining()
        }

        /**
         * calling this close means that you are expecting no more user data to
         * arrive and that there is or will be control data indicating that the
         * writing side has closed
         */
        override fun doClose()
        {
            while (true)
            {
                if (sourceQueue.isNotEmpty())
                {
                    throw IllegalStateException("there is still data to read")
                }

                if (!demultiplexedInputStreams.values.contains(this))
                {
                    return
                }

                awaitPacketArrivalAndProcessing()
            }
        }
    }
}
