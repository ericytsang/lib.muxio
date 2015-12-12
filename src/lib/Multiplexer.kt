package lib

import java.io.Closeable
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.ConnectException
import java.nio.ByteBuffer
import java.util.LinkedHashMap
import java.util.concurrent.LinkedBlockingQueue
import kotlin.properties.Delegates.observable

/**
 * Created by Eric Tsang on 12/12/2015.
 */

class Multiplexer private constructor(
    private val _inputStream:InputStream,
    private val outputStream:OutputStream):Closeable
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
     * maps IDs to the associated [LinkedBlockingQueue] of data.
     */
    private val demuxedStreams = LinkedHashMap<Short,MyInputStream>()

    /**
     * blocks until the connect accepted, or the streams are closed.
     * once accepted by the remote [Multiplexer], the method returns with a
     * [StreamPair] used to communicate with the remote [StreamPair] returned
     * when the remote [Multiplexer] called [accept].
     */
    fun connect(port:Short):Pair<InputStream,OutputStream>
    {
        val demuxedStreamPair = Pair(MyInputStream(),MyOutputStream(outputStream,port))

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

        // todo: send a connect request to the remote multiplexer
//        controlOutputStream.

        // wait for an accept packet to arrive at the input stream
        demuxedStreamPair.first.waitForConnected()

        // construct and return a stream pair
        return demuxedStreamPair
    }

    /**
     * blocks until a connect request is received, or the streams are closed.
     * when a connect request is received from the remote [Multiplexer], the
     * method returns with a [StreamPair] used to communicate with the remote
     * [StreamPair] that was returned when the remote [Multiplexer] called
     * [connect].
     */
    fun accept():Pair<InputStream,OutputStream>
    {
    }

    override fun close()
    {

    }

    private inner class ReadThread:Thread()
    {
        init
        {
            isDaemon = true
        }

        override fun run()
        {
            while (true)
            {
                val key = inputStream.readShort()
                val len = inputStream.readInt()
                val data = ByteArray(len)
                inputStream.readFully(data)
                synchronized(demuxedStreams)
                {
                    if (demuxedStreams.containsKey(key))
                    {
                        // todo: parse received data, and handle them
                        val inputStream = demuxedStreams[key]!!
                        inputStream.data.put(data)
                    }
                }
            }
        }
    }
}

// todo: implement read functions
private class MyInputStream():InputStream()
{
    val data = LinkedBlockingQueue<ByteArray>()

    var currentData:ByteBuffer = ByteBuffer.wrap(ByteArray(0))

    var notifiedOnIsConnectedChanged = Object()

    var isConnected:Boolean by observable(false)
    {
        value,old,new ->
        synchronized(notifiedOnIsConnectedChanged)
        {
            notifiedOnIsConnectedChanged.notify()
        }
    }

    fun waitForConnected()
    {
        synchronized(notifiedOnIsConnectedChanged)
        {
            if(isConnected)
            {
                return
            }
            else
            {
                notifiedOnIsConnectedChanged.wait()
            }
        }
    }

    override fun read():Int
    {
        throw UnsupportedOperationException()
    }

    override fun read(b:ByteArray?):Int
    {
        return super.read(b)
    }

    override fun read(b:ByteArray?,off:Int,len:Int):Int
    {
        return super.read(b,off,len)
    }
}

private class MyOutputStream(private val _outputStream:OutputStream,val port:Short):OutputStream()
{
    val outputStream = DataOutputStream(_outputStream)

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
        synchronized(_outputStream)
        {
            // if we are given header information, write to the stream using it
            outputStream.writeShort(port.toInt())
            outputStream.writeInt(len)
            outputStream.write(b,off,len)
        }
    }
}
