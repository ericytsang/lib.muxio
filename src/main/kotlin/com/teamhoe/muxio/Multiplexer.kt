package com.teamhoe.muxio

import java.io.DataOutputStream
import java.io.OutputStream
import java.net.ConnectException
import java.nio.ByteBuffer
import java.util.LinkedHashMap

/**
 * that receives and de-multiplexes data from the [inputStream], and multiplexes
 * data to send out the [outputStream]. provides methods ([connect] and
 * [accept]) which can be used to create an [InputStream] to read de-multiplexed
 * data from the [Multiplexer]'s [inputStream], and an [OutputStream] which can
 * be used to write multiplexed data out the [Multiplexer]'s [outputStream].
 *
 * Created by Eric Tsang on 12/12/2015.
 */
class Multiplexer(val outputStream:OutputStream)
{
    /**
     * underlying [DataOutputStream] that data is written and multiplexed into.
     */
    private val multiplexedOutputStream = DataOutputStream(outputStream)

    /**
     * underlying [DataOutputStream] that data is written and multiplexed into.
     */
    private val preMultiplexedOutputStreams = LinkedHashMap<Long,OutputStream>()

    /**
     * convenience method. automatically picks an unused port to connect to the
     * remote [Multiplexer] from.
     */
    fun connect():OutputStream
    {
        for(port in Long.MIN_VALUE..Long.MAX_VALUE)
        {
            try
            {
                return connect(port)
            }
            catch(ex:ConnectException)
            {
                // port is occupied...
            }
        }
        throw ConnectException("all ports are in use...")
    }

    /**
     * creates a output stream that maps to an input stream on the remote
     * demultiplexer.
     *
     * @param srcPort internal identifier for the demultiplexed stream. there can
     * only be one active demultiplexed stream per port at any time.
     */
    fun connect(srcPort:Long):OutputStream = synchronized(preMultiplexedOutputStreams)
    {
        if (!preMultiplexedOutputStreams.containsKey(srcPort))
        {
            val outputStream = MyOutputStream(srcPort)
            preMultiplexedOutputStreams.put(srcPort,outputStream)

            // send a connect request to the remote multiplexer
            sendConnect(srcPort)

            return outputStream
        }
        else
        {
            throw ConnectException("port is occupied...")
        }
    }

    private fun sendConnect(port:Long)
    {
        synchronized(multiplexedOutputStream)
        {
            multiplexedOutputStream.writeShort(MessageType.CONNECT.ordinal)
            multiplexedOutputStream.writeLong(port)
        }
    }

    private fun sendCloseRemote(port:Long)
    {
        synchronized(multiplexedOutputStream)
        {
            multiplexedOutputStream.writeShort(MessageType.CLOSE.ordinal)
            multiplexedOutputStream.writeLong(port)
        }
    }

    private inner class MyOutputStream(val port:Long):MultiplexingOutputStream(multiplexedOutputStream)
    {
        override fun makeHeader(b:ByteArray,off:Int,len:Int):ByteArray
        {
            return ByteBuffer.allocate(14)
                .putShort(MessageType.DATA.ordinal.toShort())
                .putLong(port)
                .putInt(len)
                .array()
        }

        override fun close()
        {
            super.close()
            sendCloseRemote(port)
            preMultiplexedOutputStreams.remove(port)
        }
    }
}
