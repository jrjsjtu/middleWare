package io.openmessaging.demo.JRJSer;


import java.nio.ByteBuffer;

/**
 * Created by jrj on 17-5-15.
 */
public class typeStruct {
    // 1 -> int, 2->long, 3->double, 4->string
    static public byte[] getByteArray(int value,String word){
        // type(char,2)+int(4)+ keylen(int,4) + key
        ByteBuffer byteBuffer = ByteBuffer.allocate(10+word.length());
        byteBuffer.putChar('1');
        byteBuffer.putInt(value);
        byteBuffer.putInt(word.length());
        byteBuffer.put(word.getBytes());
        return byteBuffer.array();
    }

    static public byte[] getByteArray(long value,String word){
        // type(char,2)+long(8)+ keylen(int,4) + key
        ByteBuffer byteBuffer = ByteBuffer.allocate(14+word.length());
        byteBuffer.putChar('2');
        byteBuffer.putLong(value);
        byteBuffer.putInt(word.length());
        byteBuffer.put(word.getBytes());
        return byteBuffer.array();
    }

    static public byte[] getByteArray(double value,String word){
        // type(char,2)+double(8)+ keylen(int,4) + key
        ByteBuffer byteBuffer = ByteBuffer.allocate(14+word.length());
        byteBuffer.putChar('3');
        byteBuffer.putDouble(value);
        byteBuffer.putInt(word.length());
        byteBuffer.put(word.getBytes());
        return byteBuffer.array();
    }

    static public byte[] getByteArray(String value,String word){
        // type(char,2)+valuelen(int,4)+ key + keylen(int,4) + key
        ByteBuffer byteBuffer = ByteBuffer.allocate(10+word.length()+value.length());
        byteBuffer.putChar('4');
        byteBuffer.putInt(value.length());
        byteBuffer.put(value.getBytes());
        byteBuffer.putInt(word.length());
        byteBuffer.put(word.getBytes());
        return byteBuffer.array();
    }
}

