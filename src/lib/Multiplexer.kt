package lib

import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.ConnectException
import java.nio.ByteBuffer
import java.util.LinkedHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Semaphore

/**
 * that receives and de-multiplexes data from the [inputStream], and multiplexes
 * data to send out the [outputStream]. provides methods ([connect] and
 * [accept]) which can be used to create an [InputStream] to read de-multiplexed
 * data from the [Multiplexer]'s [inputStream], and an [OutputStream] which can
 * be used to write multiplexed data out the [Multiplexer]'s [outputStream].
 *
 * Created by Eric Tsang on 12/12/2015.
 */
class Multiplexer private constructor(
    val inputStream:InputStream,
    val outputStream:OutputStream)
{
    companion object
    {
        /**
         * creates a [Multiplexer] that receives and de-multiplexes data from
         * the [inputStream], and multiplexes data to send out the
         * [outputStream].
         *
         * @param inputStream used to receive multiplexed data from a
         * corresponding [Multiplexer] object.
         * @param outputStream used to send data to a corresponding
         * [Multiplexer] object.
         */
        fun wrap(inputStream:InputStream,outputStream:OutputStream):Multiplexer =
            Multiplexer(inputStream,outputStream)

        /**
         * convenience method.
         */
        fun wrap(streamPair:Pair<InputStream,OutputStream>):Multiplexer =
            Multiplexer(streamPair.first,streamPair.second)
    }

    /**
     * reads multiplexed data from the [Multiplexer]'s [inputStream], then
     * parses and multiplexes the received data to the appropriate [InputStream]
     * stored in [demultiplexedStreamPairs],
     */
    private val readThread = ReadThread()

    init
    {
        readThread.start()
    }

    /**
     * underlying [DataInputStream] that data is read and de-multiplexed from.
     */
    private val multiplexedInputStream = DataInputStream(inputStream)

    /**
     * underlying [DataOutputStream] that data is written and multiplexed into.
     */
    private val multiplexedOutputStream = DataOutputStream(outputStream)

    /**
     * maps port numbers to the associated [Pair] of [InputStream]s and
     * [OutputStream]s.
     */
    private val demultiplexedStreamPairs = LinkedHashMap<Short,Pair<BlockingQueueInputStream,MultiplexingOutputStream>>()

    /**
     * maps port numbers to the associated [Object] being [Object.wait]ed on
     * that which should be notified when the connection is accepted.
     */
    private val releasedOnAccept = LinkedHashMap<Short,Semaphore>()

    /**
     * queue of received connection requests that have yet to be accepted.
     */
    private val receivedConnectRequests = LinkedBlockingQueue<Short>()

    /**
     * convenience method. automatically picks an unused port to connect to the
     * remote [Multiplexer] by.
     */
    fun connect():Pair<InputStream,OutputStream>
    {
        synchronized(demultiplexedStreamPairs)
        {
            for(port in Short.MIN_VALUE..Short.MAX_VALUE)
            {
                try
                {
                    return connect(port.toShort())
                }
                catch(ex:ConnectException)
                {
                    // part of the normal flow...
                }
            }
            throw ConnectException("all ports are in use...")
        }
    }

    /**
     * blocks until the connect request is accepted. once accepted by the remote
     * [Multiplexer], the method returns with a [Pair] of streams used to
     * communicate with the remote [Pair] of streams returned when the remote
     * [Multiplexer] called [accept].
     *
     * @param port internal identifier for the demultiplexed stream. there can
     * only be one active demultiplexed stream per port at any time.
     */
    fun connect(port:Short):Pair<InputStream,OutputStream>
    {
        val demuxedStreamPair = Pair(MyInputStream(port),MyOutputStream(port))

        // try to create a local sink to receive data for the stream pair
        synchronized(demultiplexedStreamPairs)
        {
            if (!demultiplexedStreamPairs.containsKey(port))
            {
                demultiplexedStreamPairs.put(port,demuxedStreamPair)
            }
            else
            {
                throw ConnectException("port number $port is already in use")
            }
        }

        // prepare to wait for acceptance
        prepareWaitUntilAccepted(port)

        // send a connect request to the remote multiplexer
        sendConnect(port)

        // wait until the connection is accepted or interrupted
        waitUntilAccepted(port)

        return demuxedStreamPair
    }

    /**
     * blocks until a connect request is received, or the streams are closed.
     * when a connect request is received from the remote [Multiplexer], the
     * method returns with a [Pair] of streams used to communicate with the
     * remote [Pair] of streams that was returned when the remote [Multiplexer]
     * called [connect].
     */
    fun accept():Pair<InputStream,OutputStream>
    {
        val port = receivedConnectRequests.take()
        val demuxedStreamPair = Pair(MyInputStream(port),MyOutputStream(port))

        // try to create a local sink to receive data for the stream pair
        synchronized(demultiplexedStreamPairs)
        {
            if (!demultiplexedStreamPairs.containsKey(port))
            {
                demultiplexedStreamPairs.put(port,demuxedStreamPair)
            }
            else
            {
                throw ConnectException("port number $port is already in use")
            }
        }

        // send a accept packet to remote
        sendAccept(port);

        return demuxedStreamPair
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

    private fun prepareWaitUntilAccepted(port:Short)
    {
        // acquire from the semaphore for the port
        synchronized(releasedOnAccept)
        {
            releasedOnAccept[port] = Semaphore(0)
        }
    }

    private fun waitUntilAccepted(port:Short)
    {
        try
        {
            // acquire from the semaphore for the port
            val sem = synchronized(releasedOnAccept,{releasedOnAccept[port]})
            sem ?: throw NullPointerException("need to call prepareWaitUntilAccepted first")
            sem.acquireUninterruptibly()
        }
        finally
        {
            releasedOnAccept.remove(port)
        }
    }

    private fun accept(port:Short)
    {
        try
        {
            // release on the semaphore for the port
            val sem = synchronized(releasedOnAccept,{releasedOnAccept[port]})
            sem?.release()
        }
        catch(ex:InterruptedException)
        {
            releasedOnAccept.remove(port)
            throw ex
        }
    }

    private fun sendConnect(port:Short)
    {
        synchronized(multiplexedOutputStream)
        {
            multiplexedOutputStream.writeShort(Type.CONNECT.ordinal)
            multiplexedOutputStream.writeShort(port.toInt())
        }
    }

    private fun receiveConnect()
    {
        synchronized(multiplexedInputStream)
        {
            val port = multiplexedInputStream.readShort()
            receivedConnectRequests.put(port)
        }
    }

    private fun sendAccept(port:Short)
    {
        synchronized(multiplexedOutputStream)
        {
            multiplexedOutputStream.writeShort(Type.ACCEPT.ordinal)
            multiplexedOutputStream.writeShort(port.toInt())
        }
    }

    private fun receiveAccept()
    {
        synchronized(multiplexedInputStream)
        {
            val port = multiplexedInputStream.readShort()
            accept(port)
        }
    }

    private fun makeDataHeader(port:Short,len:Int):ByteArray
    {
        return ByteBuffer.allocate(8)
            .putShort(Type.DATA.ordinal.toShort())
            .putShort(port)
            .putInt(len)
            .array()
    }

    private fun receiveData()
    {
        synchronized(multiplexedInputStream)
        {
            val key = multiplexedInputStream.readShort()
            val len = multiplexedInputStream.readInt()
            val data = ByteArray(len)
            multiplexedInputStream.readFully(data)
            val streamPair = demultiplexedStreamPairs[key] ?: return
            if(!streamPair.first.isClosed) streamPair.first.sourceQueue.put(data)
        }
    }

    private fun sendCloseRemote(port:Short)
    {
        synchronized(multiplexedOutputStream)
        {
            multiplexedOutputStream.writeShort(Type.CLOSE.ordinal)
            multiplexedOutputStream.writeShort(port.toInt())
        }
    }

    private fun receiveCloseRemote()
    {
        // close the specified stream
        synchronized(multiplexedInputStream)
        {
            val port = multiplexedInputStream.readShort()
            val streamPair = demultiplexedStreamPairs[port] ?: return

            // close the specified stream pair, and remove it from the map
            // if both its input and output streams are closed
            streamPair.first.close()

            removeStreamPairIfClosed(port)
        }
    }

    private fun removeStreamPairIfClosed(port:Short)
    {
        synchronized(demultiplexedStreamPairs)
        {
            val streamPair = demultiplexedStreamPairs[port] ?: return
            if (streamPair.first.isClosed && streamPair.second.isClosed)
            {
                demultiplexedStreamPairs.remove(port)
            }
        }
    }

    private inner class ReadThread:Thread("ReadThread")
    {
        init
        {
            isDaemon = true
        }

        override fun run()
        {
            while (true)
            {
                try
                {
                    // read header from stream
                    val type = Type.values()[multiplexedInputStream.readShort().toInt()]

                    // parse data depending on header
                    when (type)
                    {
                        Type.CONNECT -> receiveConnect()
                        Type.ACCEPT -> receiveAccept()
                        Type.DATA -> receiveData()
                        Type.CLOSE -> receiveCloseRemote()
                    }
                }
                catch(ex:InterruptedException)
                {
                    return
                }
            }
        }
    }

    private inner class MyInputStream(val port:Short):BlockingQueueInputStream()
    {
        override fun close()
        {
            super.close()
            removeStreamPairIfClosed(port)
        }
    }

    private inner class MyOutputStream(val port:Short):MultiplexingOutputStream(multiplexedOutputStream)
    {
        override fun makeHeader(b:ByteArray,off:Int,len:Int):ByteArray
        {
            return makeDataHeader(port,len)
        }

        override fun close()
        {
            super.close()
            sendCloseRemote(port)
            removeStreamPairIfClosed(port)
        }
    }

    private enum class Type
    {
        CONNECT, ACCEPT, DATA, CLOSE
    }
}
