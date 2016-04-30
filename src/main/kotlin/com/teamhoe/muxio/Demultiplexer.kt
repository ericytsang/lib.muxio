package com.teamhoe.muxio

import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.ConnectException
import java.nio.ByteBuffer
import java.util.LinkedHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Semaphore
import kotlin.concurrent.thread

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

    /**
     * maps remote source port numbers to the associated [InputStream]s.
     */
    private val demultiplexedInputStreams = LinkedHashMap<Long,BlockingQueueInputStream>()

    /**
     * queue of input streams waiting to be accepted. they may already contain
     * data ready to be read, or even closed.
     */
    private val pendingInputStreams = LinkedBlockingQueue<BlockingQueueInputStream>()

    fun accept():InputStream
    {
        return pendingInputStreams.take()
    }

    /**
     * interrupts and terminates [Multiplexer.readThread]
     */
    fun shutdown()
    {
        readThread.interrupt()
        readThread.join()
    }

    /**
     * returns true when [Multiplexer.readThread] is alive; false otherwise.]
     */
    val isShutdown:Boolean
        get() = readThread.isAlive

    private fun receiveConnect() = synchronized(demultiplexedInputStreams)
    {
        val port = multiplexedInputStream.readLong()
        if (!demultiplexedInputStreams.containsKey(port))
        {
            val inputStream = BlockingQueueInputStream(LinkedBlockingQueue())
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
        demultiplexedInputStreams.remove(port)?.isClosed = true
    }

    /**
     * reads multiplexed data from the [Multiplexer]'s [inputStream], then
     * parses and multiplexes the received data to the appropriate [InputStream]
     * stored in [demultiplexedInputStreams],
     */
    private val readThread = thread(name = "ReadThread")
    {
        while (true)
        {
            try
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
            catch(ex:InterruptedException)
            {
                return@thread
            }
        }
    }
}
