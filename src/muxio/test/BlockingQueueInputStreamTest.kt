package muxio.test

import muxio.lib.BlockingQueueInputStream
import org.junit.Test
import java.io.EOFException
import java.io.InterruptedIOException
import kotlin.concurrent.thread
import kotlin.test.assertEquals

class BlockingQueueInputStreamTest
{
    @Test
    fun testCloseCausesEOFException()
    {
        val ins = BlockingQueueInputStream()
        var success = false
        val t1 = thread()
        {
            Thread.sleep(100)
            ins.close()
        }
        val t2 = thread()
        {
            try
            {
                ins.read()
            }
            catch(ex:EOFException)
            {
                success = true
            }
        }
        t1.join()
        t2.join()
        assertEquals(true,success)
    }

    @Test
    fun testInterruptCausesInterruptException()
    {
        val ins = BlockingQueueInputStream()
        var success = false
        val t1 = thread()
        {
            try
            {
                ins.read()
            }
            catch(ex:InterruptedIOException)
            {
                success = true
            }
        }
        val t2 = thread()
        {
            Thread.sleep(100)
            t1.interrupt()
        }
        t1.join()
        t2.join()
        assertEquals(true,success)
    }

    @Test
    fun testReadReadsMoreDataThanAvailable()
    {
        val ins = BlockingQueueInputStream()
        var readResult = 0
        val t1 = thread()
        {
            ins.sourceQueue.put(byteArrayOf(1,1,1,1,1,1,1,1,1,1))
            readResult = ins.read(ByteArray(15))
        }
        val t2 = thread()
        {
            Thread.sleep(100)
            t1.interrupt()
        }
        t1.join()
        t2.join()
        assertEquals(10,readResult)
    }

    @Test
    fun testReadReadsLessDataThanAvailable()
    {
        val ins = BlockingQueueInputStream()
        var readResult = 0
        val t1 = thread()
        {
            ins.sourceQueue.put(byteArrayOf(1,1,1,1,1,1,1,1,1,1))
            ins.sourceQueue.put(byteArrayOf(1,1,1,1,1,1,1,1,1,1))
            readResult = ins.read(ByteArray(5))
        }
        val t2 = thread()
        {
            Thread.sleep(100)
            t1.interrupt()
        }
        t1.join()
        t2.join()
        assertEquals(5,readResult)
    }

    @Test
    fun testAvailable()
    {
        val ins = BlockingQueueInputStream()
        var result = 0
        val t1 = thread()
        {
            ins.sourceQueue.put(byteArrayOf(1,1,1,1,1,1,1,1,1,1))
            ins.sourceQueue.put(byteArrayOf(1,1,1,1,1,1,1,1,1,1))
            ins.read(ByteArray(5))
            result = ins.available()
        }
        val t2 = thread()
        {
            Thread.sleep(100)
            t1.interrupt()
        }
        t1.join()
        t2.join()
        assertEquals(15,result)
    }
}
