package lib

import java.io.IOException
import java.io.InterruptedIOException
import java.io.OutputStream
import kotlin.concurrent.currentThread

/**
 * Created by Eric Tsang on 12/13/2015.
 */
class MultiplexingOutputStream(
    val multiplexedOutputStream:OutputStream,
    val headerFactory:((b:ByteArray,off:Int,len:Int)->ByteArray)? = null,
    val closeListener:(()->Unit)? = null):
    OutputStream()
{
    var isClosed = false

    private var writeThread:Thread? = null

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
        try
        {
            writeThread = currentThread

            if (!isClosed)
            {
                val header = headerFactory?.invoke(b,off,len) ?: ByteArray(0)
                synchronized(multiplexedOutputStream)
                {
                    multiplexedOutputStream.write(header)
                    multiplexedOutputStream.write(b,off,len)
                }
            }
            else
            {
                throw IOException("stream is closed; cannot write.")
            }
        }

        // catch InterruptedException as it might have been thrown as a
        // result of "close" being called
        catch(ex:InterruptedException)
        {
            if (isClosed)
            {
                throw IOException("stream is closed; cannot write.")
            }
            else
            {
                throw InterruptedIOException()
            }
        }

        // set write thread to null, so we don't interrupt it unnecessarily
        finally
        {
            writeThread = null
        }
    }

    override fun close()
    {
        isClosed = true
        writeThread?.interrupt()
        closeListener?.invoke()
    }
}
