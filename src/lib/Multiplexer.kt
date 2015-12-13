package lib

import java.io.Closeable
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.InputStream
import java.io.OutputStream
import java.net.ConnectException
import java.nio.ByteBuffer
import java.util.LinkedHashMap
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.currentThread
import kotlin.properties.Delegates.observable

/**
 * Created by Eric Tsang on 12/12/2015.
 */

class Multiplexer private constructor(
    private val _inputStream:InputStream,
    private val _outputStream:OutputStream)
{
    companion object
    {
        fun wrap(inputStream:InputStream,outputStream:OutputStream):Multiplexer =
            Multiplexer(inputStream,outputStream)

        fun wrap(streamPair:Pair<InputStream,OutputStream>):Multiplexer =
            Multiplexer(streamPair.first,streamPair.second)
    }

    init
    {
        ReadThread().start()
    }

    /**
     * underlying [DataInputStream] that data is read and de-multiplexed from.
     */
    private val inputStream = DataInputStream(_inputStream)

    /**
     * underlying [DataOutputStream] that data is written and multiplexed into.
     */
    private val outputStream = DataOutputStream(_outputStream)

    /**
     * maps IDs to the associated [LinkedBlockingQueue] of data.
     */
    private val demuxedStreams = LinkedHashMap<Short,MyInputStream>()

    /**
     * queue of received connection requests that have yet to be accepted
     */
    private val receivedConnectRequests = LinkedBlockingQueue<Short>()

    /**
     * blocks until the connect accepted, or the streams are closed.
     * once accepted by the remote [Multiplexer], the method returns with a
     * [Pair] of streams used to communicate with the remote [Pair] of streams
     * returned when the remote [Multiplexer] called [accept].
     */
    fun connect(port:Short):Pair<InputStream,OutputStream>
    {
        val demuxedStreamPair = Pair(MyInputStream(),MyOutputStream(port))

        // try to create a local sink to receive data for the stream pair
        synchronized(demuxedStreams)
        {
            if (!demuxedStreams.containsKey(port))
            {
                demuxedStreams.put(port,demuxedStreamPair.first)
            }
            else
            {
                throw ConnectException("port number $port is already in use")
            }
        }

        // send a connect request to the remote multiplexer
        sendConnect(port)

        // wait for an accept packet to arrive at the input stream
        demuxedStreamPair.first.waitUntilConnected()

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
        val demuxedStreamPair = Pair(MyInputStream(),MyOutputStream(port))

        // try to create a local sink to receive data for the stream pair
        synchronized(demuxedStreams)
        {
            if (!demuxedStreams.containsKey(port))
            {
                demuxedStreams.put(port,demuxedStreamPair.first)
            }
            else
            {
                throw ConnectException("port number $port is already in use")
            }
        }

        // set isConnected to true...
        demuxedStreams[port]!!.isConnected = true

        // send a accept packet to remote
        sendAccept(port);

        return demuxedStreamPair
    }

    // todo: implement...
    fun close(port:Short)
    {

    }

    private fun sendConnect(port:Short)
    {
        synchronized(outputStream)
        {
            outputStream.writeShort(Type.CONNECT.ordinal)
            outputStream.writeShort(port.toInt())
        }
    }

    private fun receiveConnect()
    {
        synchronized(inputStream)
        {
            val port = inputStream.readShort()
            receivedConnectRequests.put(port)
        }
    }

    private fun sendAccept(port:Short)
    {
        synchronized(outputStream)
        {
            outputStream.writeShort(Type.ACCEPT.ordinal)
            outputStream.writeShort(port.toInt())
        }
    }

    private fun receiveAccept()
    {
        synchronized(inputStream)
        {
            val port = inputStream.readShort()
            demuxedStreams[port]!!.isConnected = true
        }
    }

    private fun sendData(port:Short,data:ByteArray,off:Int,len:Int)
    {
        synchronized(outputStream)
        {
            outputStream.writeShort(Type.DATA.ordinal)
            outputStream.writeShort(port.toInt())
            outputStream.writeInt(len)
            outputStream.write(data,off,len)
        }
    }

    private fun receiveData()
    {
        synchronized(inputStream)
        {
            val key = inputStream.readShort()
            val len = inputStream.readInt()
            val data = ByteArray(len)
            inputStream.readFully(data)
            synchronized(demuxedStreams)
            {
                if (demuxedStreams.containsKey(key))
                {
                    val inputStream = demuxedStreams[key]!!
                    inputStream.data.put(data)
                }
            }
        }
    }

    private fun sendClose(port:Short)
    {
        synchronized(outputStream)
        {
            outputStream.writeShort(Type.CLOSE.ordinal)
            outputStream.writeShort(port.toInt())
        }
    }

    private fun receiveClose()
    {
        synchronized(inputStream)
        {
            val key = inputStream.readShort()
            synchronized(demuxedStreams)
            {
                if (demuxedStreams.containsKey(key))
                {
                    val inputStream = demuxedStreams[key]!!
                    inputStream.isConnected = false
                }
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
                // read header from stream
                val type = Type.values()[inputStream.readShort().toInt()]

                // parse data depending on header
                when (type)
                {
                    Type.CONNECT -> receiveConnect()
                    Type.ACCEPT -> receiveAccept()
                    Type.DATA -> receiveData()
                    Type.CLOSE -> receiveClose()
                }
            }
        }
    }

    // todo: refactor to simplify
    private inner class MyInputStream:InputStream()
    {
        val data = LinkedBlockingQueue<ByteArray>()

        var currentData:ByteBuffer = ByteBuffer.wrap(ByteArray(0))

        var notifiedOnIsConnectedChanged = Object()

        var isConnected:Boolean = false
            set(value)
            {
                field = value
                synchronized(notifiedOnIsConnectedChanged)
                {
                    notifiedOnIsConnectedChanged.notify()
                    readingThread?.interrupt()
                }
            }

        var readingThread:Thread? = null

        fun waitUntilConnected()
        {
            synchronized(notifiedOnIsConnectedChanged)
            {
                while(!isConnected) notifiedOnIsConnectedChanged.wait()
            }
        }

        override fun read():Int
        {
            synchronized(this)
            {
                // make sure there is data in the currentData to consume, or throw
                // something
                try
                {
                    readingThread = currentThread
                    while (!currentData.hasRemaining())
                    {
                        try
                        {
                            // take the next chunk of data as the current packet to
                            // read from if there could potentially be "a next chunk
                            // of data".
                            if(isConnected || (!isConnected && !data.isEmpty()))
                            {
                                currentData = ByteBuffer.wrap(data.take())
                            }

                            // no data available; EOF condition is met, throw
                            // EOFException.
                            else
                            {
                                throw EOFException()
                            }
                        }
                        catch(ex:InterruptedException)
                        {
                            // throw EOFException if EOF condition is met.
                            if (!isConnected && data.isEmpty())
                            {
                                throw EOFException()
                            }

                            // propagate the exception up otherwise...maye user
                            // interrupted the thread on purpose.
                            else
                            {
                                throw ex
                            }
                        }
                    }
                }
                finally
                {
                    readingThread = null
                }

                // return the next byte of data
                return currentData.get().toInt()
            }
        }
    }

    private inner class MyOutputStream(val port:Short):OutputStream()
    {
        override fun write(b:Int)
        {
            write(byteArrayOf(b.toByte()))
        }

        override fun write(b:ByteArray)
        {
            write(b,0,b.size)
        }

        override fun write(b:ByteArray,off:Int,len:Int)
        {
            sendData(port,b,off,len)
        }
    }
}

private enum class Type
{
    CONNECT, ACCEPT, DATA, CLOSE
}
