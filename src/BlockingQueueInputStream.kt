import java.io.EOFException
import java.io.InputStream
import java.io.InterruptedIOException
import java.nio.ByteBuffer
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

/**
 * [InputStream] that wraps any [BlockingQueue]. it is a [InputStream] adapter
 * for a [BlockingQueue]; data put into the [sourceQueue] may be read out of
 * this [BlockingQueueInputStream].
 *
 * Created by Eric Tsang on 12/13/2015.
 */
open class BlockingQueueInputStream(
    val sourceQueue:BlockingQueue<ByteArray> = LinkedBlockingQueue<ByteArray>()):
    InputStream()
{
    /**
     * true when the socket is closed; false otherwise.
     */
    var isClosed:Boolean = false
        private set

    /**
     * reference to the thread that is reading from this
     * [BlockingQueueInputStream] because the thread could be blocked; may need
     * interrupting if [close] is called.
     */
    private var readingThread:Thread? = null

    /**
     * reference to the data last taken from [sourceQueue]. a call to [read] may
     * have only partially consumed it; we need to maintain a reference to it so
     * we may feed the rest of it to subsequent calls to [read].
     */
    private var currentData:ByteBuffer = ByteBuffer.wrap(ByteArray(0))

    /**
     * convenience method
     */
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

    /**
     * convenience method
     */
    override fun read(b:ByteArray):Int
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
                    readingThread = Thread.currentThread()

                    when
                    {
                    // take the next ByteArray as the currentData to read from
                    // if there could potentially be or is a next ByteArray.
                        !isClosed && sourceQueue.isEmpty() ->
                            currentData = ByteBuffer.wrap(sourceQueue.take())
                        sourceQueue.isNotEmpty() ->
                            currentData = ByteBuffer.wrap(sourceQueue.poll()!!)

                    // no data available; EOF condition is met, throw
                    // EOFException.
                        isClosed && sourceQueue.isEmpty() ->
                            throw EOFException()
                    }
                }
                catch(ex:InterruptedException)
                {
                    // could have been interrupted by the call to close;
                    // throw EOFException if EOF condition is met.
                    if (isClosed && sourceQueue.isEmpty())
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

    /**
     * Returns an estimate of the number of bytes that can be read (or skipped
     * over) from this input stream without blocking by the next invocation of a
     * method for this input stream. The next invocation might be the same
     * thread or another thread. A single read or skip of this many bytes will
     * not block, but may read or skip fewer bytes.
     */
    override fun available():Int
    {
        return sourceQueue.sumBy{it.size}+currentData.remaining()
    }

    /**
     * Closes this input stream and releases any system resources associated
     * with the stream. any calls to read will consume and return the remaining
     * buffered data within this stream. once all buffered data has been
     * consumed, calls to read will throw [EOFException]s. in the case that
     * there is already no buffered data in the stream, and [close] is called,
     * the current call to [read] will throw an [EOFException].
     */
    override fun close()
    {
        isClosed = true
        readingThread?.interrupt()
    }
}