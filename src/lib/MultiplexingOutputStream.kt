package lib

import java.io.Closeable
import java.io.IOException
import java.io.OutputStream
import kotlin.concurrent.currentThread

/**
 * Created by Eric Tsang on 12/13/2015.
 */
class MultiplexingOutputStream(
    val sharedStream:OutputStream,
    val sendHeader:(sharedStream:OutputStream,b:ByteArray,off:Int,len:Int)->Unit = {stream,b,off,len->ByteArray(0)},
    val closeListener:(()->Unit)?):
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
        synchronized(sharedStream)
        {
            try
            {
                writeThread = currentThread

                if (!isClosed)
                {
                    sendHeader(sharedStream,b,off,len)
                    sharedStream.write(b,off,len)
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
                if(isClosed)
                {
                    throw IOException("stream is closed; cannot write.")
                }
                else
                {
                    throw ex
                }
            }

            finally
            {
                writeThread = null
            }
        }
    }

    override fun close()
    {
        isClosed = true
        writeThread?.interrupt()
        closeListener?.invoke()
    }
}
