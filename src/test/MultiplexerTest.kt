package test

import lib.Multiplexer
import org.junit.Test
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.net.ConnectException
import kotlin.concurrent.thread
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Created by Eric on 12/14/2015.
 */
class MultiplexerTest
{
    @Test
    fun testConnectAndAccept()
    {
        val outs1 = PipedOutputStream()
        val ins1 = PipedInputStream(outs1,100)

        val outs2 = PipedOutputStream()
        val ins2 = PipedInputStream(outs2,100)

        val mux1 = Multiplexer.wrap(ins1,outs2)
        val mux2 = Multiplexer.wrap(ins2,outs1)

        val t1 = thread()
        {
            mux1.connect(1)
            mux1.connect(2)
            mux1.connect(3)
            mux1.connect(4)
        }

        val t2 = thread()
        {
            mux2.accept()
            mux2.accept()
            mux2.accept()
            mux2.accept()
        }

        t1.join()
        t2.join()
    }

    @Test
    fun testConnectDuplicate()
    {
        val outs1 = PipedOutputStream()
        val ins1 = PipedInputStream(outs1,100)

        val outs2 = PipedOutputStream()
        val ins2 = PipedInputStream(outs2,100)

        val mux1 = Multiplexer.wrap(ins1,outs2)
        val mux2 = Multiplexer.wrap(ins2,outs1)

        var success = false

        val t1 = thread()
        {
            try
            {
                mux1.connect(1)
                mux1.connect(1)
            }
            catch(ex:ConnectException)
            {
                success = true
            }
        }

        val t2 = thread()
        {
            mux2.accept()
        }

        t1.join()
        t2.join()

        assertTrue(success,"connecting on same port multiple times before closing doesn't throw anything")
    }

    @Test
    fun testConnectDuplicateAfterClose()
    {
        val outs1 = PipedOutputStream()
        val ins1 = PipedInputStream(outs1,100)

        val outs2 = PipedOutputStream()
        val ins2 = PipedInputStream(outs2,100)

        val mux1 = Multiplexer.wrap(ins1,outs2)
        val mux2 = Multiplexer.wrap(ins2,outs1)

        var success = false

        val t1 = thread()
        {
            val pair = mux1.connect(1)
            pair.second.close()
            mux1.connect(1)
            success = true
        }

        val t2 = thread()
        {
            try
            {
                mux2.accept().second.close()
                mux2.accept()
            }
            catch(ex:Exception)
            {
                fail("stream pair should have been closed before second accept was executed...got exception:${ex.message}")
            }
        }

        t1.join()
        t2.join()

        assertTrue(success,"connecting on same port twice after closing throws something")
    }

    @Test
    fun testGeneralMultiplexing()
    {
        val outs1 = PipedOutputStream()
        val ins1 = PipedInputStream(outs1,100)

        val outs2 = PipedOutputStream()
        val ins2 = PipedInputStream(outs2,100)

        val mux1 = Multiplexer.wrap(ins1,outs2)
        val mux2 = Multiplexer.wrap(ins2,outs1)

        val t1 = thread()
        {
            val pair1 = mux1.connect(1)
            val pair2 = mux1.connect(2)
            val pair3 = mux1.connect(3)

            val t3 = thread()
            {
                pair1.first.close()
                Thread.sleep(100)
                val outs = DataOutputStream(pair1.second)
                repeat(3000)
                {
                    outs.writeUTF("pair1")
                }
                pair1.second.close()
            }

            val t4 = thread()
            {
                pair2.first.close()
                val outs = DataOutputStream(pair2.second)
                repeat(3000)
                {
                    outs.writeUTF("pair2")
                }
                pair2.second.close()
            }

            val t5 = thread()
            {
                pair3.first.close()
                val outs = DataOutputStream(pair3.second)
                repeat(3000)
                {
                    outs.writeUTF("pair3")
                }
                pair3.second.close()
            }

            t3.join()
            t4.join()
            t5.join()
        }

        val t2 = thread()
        {
            val pair1 = mux2.accept()
            val pair2 = mux2.accept()
            val pair3 = mux2.accept()

            val t3 = thread()
            {
                pair1.second.close()
                val ins = DataInputStream(pair1.first)
                repeat(3000)
                {
                    assertTrue(ins.readUTF() == "pair1")
                }
                pair1.first.close()
            }

            val t4 = thread()
            {
                pair2.second.close()
                Thread.sleep(100)
                val ins = DataInputStream(pair2.first)
                repeat(3000)
                {
                    assertTrue(ins.readUTF() == "pair2")
                }
                pair2.first.close()
            }

            val t5 = thread()
            {
                pair3.second.close()
                Thread.sleep(100)
                val ins = DataInputStream(pair3.first)
                repeat(3000)
                {
                    assertTrue(ins.readUTF() == "pair3")
                }
                pair3.first.close()
            }

            t3.join()
            t4.join()
            t5.join()
        }

        t1.join()
        t2.join()
    }
}
