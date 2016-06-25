package com.github.ericytsng.lib.muxio

import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.io.InterruptedIOException
import java.nio.ByteBuffer
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

/**
 * [InputStream] that wraps any [BlockingQueue]. it is a [InputStream] adapter
 * for a [BlockingQueue]; data put into the [sourceQueue] may be read out of
 * this [AbstractInputStream].
 *
 * Created by Eric Tsang on 12/13/2015.
 */
abstract class AbstractInputStream:InputStream()
{
    /**
     * convenience method
     */
    final override fun read():Int
    {
        val data = ByteArray(1)
        val result = read(data)

        when (result)
        {
        // if EOF, return -1 as specified by java docs
            -1 -> return result

        // if data was actually read, return the read data
            1 -> return data[0].toInt()

        // throw an exception in all other cases
            else -> throw RuntimeException("unhandled case in when statement!")
        }
    }

    /**
     * convenience method
     */
    final override fun read(b:ByteArray):Int
    {
        return read(b,0,b.size)
    }

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
    final override fun read(b:ByteArray,off:Int,len:Int):Int = synchronized(this)
    {
        return state.read(b,off,len)
    }

    /**
     * Returns an estimate of the number of bytes that can be read (or skipped
     * over) from this input stream without blocking by the next invocation of a
     * method for this input stream. The next invocation might be the same
     * thread or another thread. A single read or skip of this many bytes will
     * not block, but may read or skip fewer bytes.
     */
    final override fun available():Int = synchronized(this)
    {
        return state.available()
    }

    /**
     * this implementation of close doesn't do anything
     */
    final override fun close() = synchronized(this)
    {
        state.close()
    }

    fun notifyEofReached() = synchronized(this)
    {
        state.notifyEofReached()
    }

    abstract fun doRead(b:ByteArray,off:Int,len:Int):Int

    abstract fun doAvailable():Int

    abstract fun doClose()

    private var state:State = DataAvailable()

    private interface State
    {
        fun read(b:ByteArray,off:Int,len:Int):Int
        fun available():Int
        fun close()
        fun notifyEofReached()
    }

    private inner class DataAvailable:State
    {
        override fun read(b:ByteArray,off:Int,len:Int):Int
        {
            return doRead(b,off,len)
        }

        override fun available():Int
        {
            return doAvailable()
        }

        override fun close()
        {
            doClose()
            state = Closed()
        }

        override fun notifyEofReached()
        {
            state = EndOfFile()
        }
    }

    private inner class EndOfFile:State
    {
        override fun read(b:ByteArray,off:Int,len:Int):Int
        {
            return -1
        }

        override fun available():Int
        {
            return 0
        }

        override fun close()
        {
            state = Closed()
        }

        override fun notifyEofReached()
        {
            throw IOException("already eof!")
        }
    }

    private inner class Closed:State
    {
        override fun read(b:ByteArray,off:Int,len:Int):Int
        {
            throw IOException("already closed!")
        }

        override fun available():Int
        {
            throw IOException("already closed!")
        }

        override fun close()
        {
            throw IOException("already closed!")
        }

        override fun notifyEofReached()
        {
            throw IOException("already closed!")
        }
    }
}