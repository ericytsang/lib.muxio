package com.teamhoe.muxio

import org.junit.Test
import java.io.DataInputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.util.Arrays
import kotlin.concurrent.thread

/**
 * Created by surpl on 4/29/2016.
 */
class MultiplexerAndDemultiplexerTest
{
    @Test
    fun testGeneral()
    {
        val dataToSend = setOf(byteArrayOf(1,1,1,1,1,1),byteArrayOf(1,3,3,3,1,1),byteArrayOf(1,1,4,4,4,4))
        val dataReceived = setOf(ByteArray(6),ByteArray(6),ByteArray(6))
        val pipedIns = PipedInputStream(100)
        val pipedOuts = PipedOutputStream(pipedIns)
        val demux = Demultiplexer(pipedIns)
        val mux = Multiplexer(pipedOuts)

        val t1 = thread()
        {
            val connections = dataToSend.associate {it to mux.connect()}
            connections.forEach {it.value.write(it.key)}
            connections.forEach {it.value.close()}
            pipedOuts.close()
        }
        val t2 = thread()
        {
            val connections = dataReceived.associate {it to demux.accept()}
            connections.forEach {DataInputStream(it.value).readFully(it.key)}
            connections.forEach {it.value.close()}
            demux.shutdown()
            pipedIns.close()
        }
        t1.join()
        t2.join()

        assert(dataReceived.map {Arrays.toString(it)}.toSet() == dataToSend.map {Arrays.toString(it)}.toSet())
    }
}
