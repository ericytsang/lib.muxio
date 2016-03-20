package muxio.test

import muxio.lib.BlockingStreamPair
import org.junit.Test
import java.io.PipedInputStream
import java.io.PipedOutputStream
import kotlin.concurrent.currentThread

import kotlin.concurrent.thread
import kotlin.test.assertTrue

/**
 * Created by Eric Tsang on 12/14/2015.
 */
class BlockingStreamPairTest
{
    @Test
    fun blocksUntilAckIsReceived()
    {
        val pipedInputStream1 = PipedInputStream(100)
        val pipedOutputStream1 = PipedOutputStream(pipedInputStream1)

        val pipedInputStream2 = PipedInputStream(100)
        val pipedOutputStream2 = PipedOutputStream(pipedInputStream2)

        val streamPair = BlockingStreamPair.wrap(pipedInputStream1,pipedOutputStream2)

        var start:Long
        var end:Long
        var elapsed:Long = 0
        val delay = 3000L

        val t1 = thread()
        {
            start = System.currentTimeMillis()
            streamPair.outputStream.write(0)
            end = System.currentTimeMillis()
            elapsed = end-start
            println("elapsed time: $elapsed ms")
        }

        val t2 = thread(isDaemon = true)
        {
            Thread.sleep(delay)
            while (!currentThread.isInterrupted)
            {
                pipedOutputStream1.write(pipedInputStream2.read())
            }
        }

        t1.join()
        t2.interrupt()

        assertTrue(elapsed >= delay)
    }
}
