import lib.Multiplexer
import java.io.DataInput
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream
import kotlin.concurrent.thread

/**
 * Created by Eric Tsang on 12/12/2015.
 */

fun main(args:Array<String>)
{
    val outs1 = PipedOutputStream()
    val ins1 = PipedInputStream(outs1,100)

    val outs2 = PipedOutputStream()
    val ins2 = PipedInputStream(outs2,100)

    val mux1 = Multiplexer.wrap(ins1,outs2)
    val mux2 = Multiplexer.wrap(ins2,outs1)

    thread()
    {
        val hi1 = mux1.connect(2)

        thread {
            hi1.first.close()
//            while(true)
//            {
//                println(DataInputStream(hi1.first).readUTF())
//            }
//            Thread.sleep(5000)
        }

        Thread.sleep(5000)
    }

    thread()
    {
        val bye1 = mux2.accept()

        thread {
            Thread.sleep(2000)
            repeat(Int.MAX_VALUE)
            {
                DataOutputStream(bye1.second).writeUTF("hello from bye1")
            }
            Thread.sleep(5000)
        }

        Thread.sleep(5000)
    }
}
