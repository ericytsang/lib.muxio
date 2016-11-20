package com.github.ericytsang.lib.muxio

import java.io.InputStream
import java.util.concurrent.BlockingQueue

/**
 * [InputStream] that wraps any [BlockingQueue]. it is a [InputStream] adapter
 * for a [BlockingQueue]; data put into the [sourceQueue] may be read out of
 * this [AbstractInputStream].
 *
 * Created by Eric Tsang on 12/13/2015.
 */
abstract class AbstractInputStream:InputStream()
{
    final override fun read():Int
    {
        val data = ByteArray(1)
        val result = read(data)

        when (result)
        {
        // if EOF, return -1 as specified by java docs
            -1 -> return result

        // if data was actually read, return the read data
            1 -> return data[0].toInt().and(0xFF)

        // throw an exception in all other cases
            else -> throw RuntimeException("unhandled case in when statement!")
        }
    }
    final override fun read(b:ByteArray):Int = read(b,0,b.size)
    final override fun read(b:ByteArray,off:Int,len:Int):Int = state.read(b,off,len)
    final override fun available():Int = 0
    final override fun close() = state.close()
    val isClosed:Boolean get() = state.isClosed

    /**
     * Reads up to len bytes of data from the input stream into an array of
     * bytes. An attempt is made to read as many as len bytes, but a smaller
     * number may be read. The number of bytes actually read is returned as an
     * integer. This method blocks until input data is available, end of file is
     * detected, or an exception is thrown.
     *
     * @param b the buffer into which the data is read.
     * @param off the start offset in array b at which the data is written.
     * @param len the maximum number of bytes to read.
     */
    protected abstract fun doRead(b:ByteArray,off:Int,len:Int):Int

    protected abstract fun doClose()

    private var state:State = Opened()

    private interface State
    {
        val isClosed:Boolean
        fun read(b:ByteArray,off:Int,len:Int):Int
        fun close()
    }

    private inner class Opened:State
    {
        override fun read(b:ByteArray,off:Int,len:Int):Int = synchronized(this)
        {
            val readResult = doRead(b,off,len)
            if (readResult == -1)
            {
                state = Closed()
            }
            return readResult
        }
        override val isClosed:Boolean get() = false
        override fun close() { state = Closed() }
    }

    private inner class Closed:State
    {
        init { doClose() }
        override fun read(b:ByteArray,off:Int,len:Int):Int = -1
        override fun close() = Unit
        override val isClosed:Boolean get() = true
    }
}