package com.github.ericytsang.lib.muxio

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
    val pipeOut = PipedOutputStream()
    val pipeIn = PipedInputStream(pipeOut)
    val mux = Multiplexer(pipeOut)
    val demux = Demultiplexer(pipeIn)

    @Test
    fun testGeneral()
    {
        val dataToSend = setOf(byteArrayOf(1,1,1,1,1,1),byteArrayOf(1,3,3,3,1,1),byteArrayOf(1,1,4,4,4,4))
        val dataReceived = setOf(ByteArray(6),ByteArray(6),ByteArray(6))

        val t1 = thread()
        {
            val connections = dataToSend.associate {it to mux.connect()}
            connections.forEach {it.value.write(it.key)}
            connections.forEach {println("closing");it.value.close();println("closed");}
            pipeOut.close()
        }
        val t2 = thread()
        {
            val connections = dataReceived.associate {it to demux.accept()}
            connections.forEach {DataInputStream(it.value).readFully(it.key)}
            connections.forEach {it.value.close()}
            pipeIn.close()
            println(dataReceived.map {Arrays.toString(it)})
        }
        t1.join()
        t2.join()

        assert(dataReceived.map {Arrays.toString(it)}.toSet() == dataToSend.map {Arrays.toString(it)}.toSet())
    }

    @Test
    fun connectSendCloseThenAcceptAndRead()
    {
        val dataSend = byteArrayOf(1,1,1,1,1,1)
        val dataRecv = ByteArray(dataSend.size)
        mux.connect().use {it.write(dataSend)}
        demux.accept().let(::DataInputStream).use {it.readFully(dataRecv)}
        assert(Arrays.equals(dataSend,dataRecv))
    }

    fun convertByteTest(payload:Int)
    {
        mux.connect().write(payload)
        val int:Int = demux.accept().let(::DataInputStream).read()
        assert(int == payload)
    }

    @Test fun convertingByteTest1() = convertByteTest(255)
    @Test fun convertingByteTest2() = convertByteTest(254)
    @Test fun convertingByteTest3() = convertByteTest(5)
    @Test fun convertingByteTest4() = convertByteTest(Byte.MAX_VALUE.toInt())
    @Test fun convertingByteTest5() = convertByteTest(Byte.MAX_VALUE.toInt()+8)
}
