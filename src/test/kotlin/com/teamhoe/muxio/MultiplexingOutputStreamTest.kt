package com.teamhoe.muxio

import org.junit.Test
import java.io.*
import java.nio.ByteBuffer
import java.util.concurrent.BlockingQueue
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Created by Eric on 12/14/2015.
 */
class MultiplexingOutputStreamTest
{
    @Test
    fun testWriteIsHeadedByHeaders()
    {
        val muxedIns = PipedInputStream(100)
        val muxedOuts = PipedOutputStream(muxedIns)
        val demuxedOuts1 = MyMultiplexingOutputStream(muxedOuts,
            {b,o,l -> ByteBuffer.allocate(4).putInt(l).array()})

        val t1 = thread()
        {
            repeat(30000)
            {
                demuxedOuts1.write(ByteArray((Math.random()*100).toInt()))
            }
        }
        val t2 = thread()
        {
            val ins = DataInputStream(muxedIns)
            repeat(30000)
            {
                val size = ins.readInt()
                val ba = ByteArray(size)
                ins.readFully(ba)
            }
        }
        t1.join()
        t2.join()
        assertEquals(0,muxedIns.available(),"write fails synchronize correctly")
    }

    @Test
    fun testWriteIsProperlySynchronizedAndMultiplexed()
    {
        val muxedIns = PipedInputStream(1000)
        val muxedOuts = PipedOutputStream(muxedIns)
        val demuxedOuts1 = MyMultiplexingOutputStream(muxedOuts,
            {b,o,l -> ByteBuffer.allocate(6).putShort(1).putInt(l).array()})
        val demuxedOuts2 = MyMultiplexingOutputStream(muxedOuts,
            {b,o,l -> ByteBuffer.allocate(6).putShort(2).putInt(l).array()})

        val t1 = thread()
        {
            var counter = 5L
            val outs = DataOutputStream(demuxedOuts1)
            repeat(30000)
            {
                outs.writeLong(counter++)
            }
        }
        val t2 = thread()
        {
            var counter = 0L
            val outs = DataOutputStream(demuxedOuts2)
            repeat(30000)
            {
                outs.writeLong(counter++)
            }
        }
        val t3 = thread()
        {
            var counter1 = 5L
            var counter2 = 0L
            val ins = DataInputStream(muxedIns)
            repeat(60000)
            {
                val src = ins.readShort()
                val size = ins.readInt()
                val ba = ByteArray(size)
                ins.readFully(ba)
                assertTrue(when (src)
                {
                    1.toShort() -> ByteBuffer.wrap(ba).long == counter1++
                    2.toShort() -> ByteBuffer.wrap(ba).long == counter2++
                    else -> throw RuntimeException("unexpected size of: $size")
                })
            }
        }
        t1.join()
        t2.join()
        t3.join()
        assertEquals(0,muxedIns.available(),"write fails synchronize correctly")
    }

    @Test
    fun testCloseCausesIOExceptionOnSubsequentWrites()
    {
        val muxedOuts = PipedOutputStream(PipedInputStream(1000))
        val demuxedOuts1 = MyMultiplexingOutputStream(muxedOuts,
            {b,o,l -> ByteBuffer.allocate(6).putShort(1).putInt(l).array()})
        var success = false

        try
        {
            demuxedOuts1.close()
            demuxedOuts1.write(byteArrayOf(1,1,1,1,1,1,1,1))
        }
        catch(ex:IOException)
        {
            success = true
        }
        finally
        {
            assertTrue(success,"fails to throw IOException on subsequent writes after call to close")
        }
    }

    @Test
    fun testBlockingWriteCanBeInterruptedByInterrupt()
    {
        val muxedOuts = PipedOutputStream(PipedInputStream(1))
        val demuxedOuts1 = MyMultiplexingOutputStream(muxedOuts,
            {b,o,l -> ByteBuffer.allocate(6).putShort(1).putInt(l).array()})
        var success = false

        val t1 = thread()
        {
            try
            {
                demuxedOuts1.write(byteArrayOf(1,1,1,1,1,1,1,1))
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
        assertTrue(success,"write fails to block, or calling close did not throw InterruptedIOException")
    }

    @Test
    fun testBlockingWriteCausesCloseToThrow()
    {
        val muxedOuts = PipedOutputStream(PipedInputStream(1))
        val demuxedOuts1 = MyMultiplexingOutputStream(muxedOuts,
            {b,o,l -> ByteBuffer.allocate(6).putShort(1).putInt(l).array()})
        var success = false

        val t1 = thread()
        {
            demuxedOuts1.write(byteArrayOf(1,1,1,1,1,1,1,1))
        }
        val t2 = thread()
        {
            try
            {
                Thread.sleep(100)
                demuxedOuts1.close()
            }
            catch(ex:IOException)
            {
                success = true
            }
        }
        t2.join()
        assertTrue(success,"write fails to block, or calling close did not throw IOException")
    }

    private class MyMultiplexingOutputStream(val outputStream:OutputStream,val headerFactory:(b:ByteArray,off:Int,len:Int)->ByteArray):
        MultiplexingOutputStream(outputStream)
    {
        override fun makeHeader(b:ByteArray,off:Int,len:Int):ByteArray
        {
            return headerFactory(b,off,len)
        }
    }
}
