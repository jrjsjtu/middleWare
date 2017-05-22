package io.openmessaging.demo.JRJSer;

/**
 * Created by Shinelon on 2017/5/21.
 */
public class JByteBuffer {
    byte[] buffer;
    public JByteBuffer(int size){
        buffer = new byte[size];
    }
    public byte[] array(){
        return buffer;
    }
    public void put(byte[] tmp,int offset,int length){
        for(int i=0,j=offset;i<length;i++){
            buffer[j] = tmp[i];
        }
    }
}
