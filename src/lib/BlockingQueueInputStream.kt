package lib

import java.io.EOFException
import java.io.InputStream
import java.io.InterruptedIOException
import java.nio.ByteBuffer
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.currentThread

/**
 * Created by Eric Tsang on 12/13/2015.
 */
open class BlockingQueueInputStream(
    val source:BlockingQueue<ByteArray> = LinkedBlockingQueue<ByteArray>()):
    InputStream()
{
    private var currentData:ByteBuffer = ByteBuffer.wrap(ByteArray(0))

    var isClosed:Boolean = false
        private set

    var readingThread:Thread? = null

    override fun read():Int
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

    override fun read(b:ByteArray):Int
    {
        return read(b,0,b.size)
    }

    override fun read(b:ByteArray,off:Int,len:Int):Int
    {
        synchronized(this)
        {
            var bytesRead = 0

            // make sure there is data in the currentData to consume blocking if
            // needed, else throw EOFException.
            if (!currentData.hasRemaining())
            {
                try
                {
                    readingThread = currentThread

                    when
                    {
                    // take the next ByteArray as the currentData to read from
                    // if there could potentially be or is a next ByteArray.
                        !isClosed && source.isEmpty() ->
                            currentData = ByteBuffer.wrap(source.take())
                        source.isNotEmpty() ->
                            currentData = ByteBuffer.wrap(source.poll()!!)

                    // no data available; EOF condition is met, throw
                    // EOFException.
                        isClosed && source.isEmpty() ->
                            throw EOFException()
                    }
                }
                catch(ex:InterruptedException)
                {
                    // could have been interrupted by the call to close;
                    // throw EOFException if EOF condition is met.
                    if (isClosed && source.isEmpty())
                    {
                        throw EOFException()
                    }

                    // propagate the exception up otherwise...maybe user
                    // interrupted the thread on purpose.
                    else
                    {
                        throw InterruptedIOException()
                    }
                }
                finally
                {
                    readingThread = null
                }
            }

            // read all remaining data into user buffer, or just until the
            // user's bytes to read requirement is met
            val bytesToRead = Math.min(len-bytesRead,currentData.remaining())
            bytesRead += bytesToRead
            currentData.get(b,off,bytesToRead)

            // return the number of bytes read
            return bytesRead
        }
    }

    override fun available():Int
    {
        return source.sumBy{it.size}+currentData.remaining()
    }

    override fun close()
    {
        isClosed = true
        readingThread?.interrupt()
    }
}