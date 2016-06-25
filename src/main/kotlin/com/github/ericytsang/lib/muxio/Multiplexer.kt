package com.github.ericytsang.lib.muxio

import java.io.DataOutputStream
import java.io.OutputStream
import java.net.ConnectException
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
    companion object
    {
        /**
         * maximum number of bytes of user data allowed per data packet.
         */
        private const val MAX_USER_DATA_LEN = 100
    }

    /**
     * underlying [DataOutputStream] that data is written and multiplexed into.
     */
    private val multiplexedOutputStream = DataOutputStream(outputStream)

    /**
     * active [OutputStream]s that need multiplexing.
     */
    private val demultiplexedOutputStreams = LinkedHashMap<Long,OutputStream>()

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
    fun connect(srcPort:Long):OutputStream = synchronized(demultiplexedOutputStreams)
    {
        if (!demultiplexedOutputStreams.containsKey(srcPort))
        {
            val outputStream = MyOutputStream(srcPort)
            demultiplexedOutputStreams.put(srcPort,outputStream)

            // send a connect request to the remote multiplexer
            synchronized(multiplexedOutputStream)
            {
                multiplexedOutputStream.writeShort(MessageType.CONNECT.ordinal)
                multiplexedOutputStream.writeLong(srcPort)
            }

            return outputStream
        }
        else
        {
            throw ConnectException("port is occupied...")
        }
    }

    private inner class MyOutputStream(val port:Long):AbstractOutputStream()
    {
        override fun doWrite(b:ByteArray,off:Int,len:Int)
        {
            var writeCursor = off
            var bytesRemaining = len
            while (bytesRemaining > 0)
            {
                synchronized(multiplexedOutputStream)
                {
                    val bytesToWrite = Math.min(bytesRemaining,MAX_USER_DATA_LEN)
                    multiplexedOutputStream.writeShort(MessageType.DATA.ordinal)
                    multiplexedOutputStream.writeLong(port)
                    multiplexedOutputStream.writeInt(bytesToWrite)
                    multiplexedOutputStream.write(b,writeCursor,bytesToWrite)
                    writeCursor += bytesToWrite
                    bytesRemaining -= bytesToWrite
                }
            }
        }

        override fun doClose()
        {
            synchronized(demultiplexedOutputStreams)
            {
                synchronized(multiplexedOutputStream)
                {
                    multiplexedOutputStream.writeShort(MessageType.CLOSE.ordinal)
                    multiplexedOutputStream.writeLong(port)
                }
                demultiplexedOutputStreams.remove(port)
            }
        }
    }
}
