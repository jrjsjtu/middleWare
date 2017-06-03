package io.openmessaging.demo;

import io.openmessaging.tester.Constants;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Created by jrj on 17-5-22.
 */
public class AsyncLogging{
    //use one thread to manage multiple files
    private static final int blockingSize = 1024*1024/2;//2MB
    private static final int ZipSize = 1024*1024/2;//2MB
    public static final int fileMagicNumber = 51205191;

    FileOutputStream out = null;

    String filePath;
    Lock lock;
    ByteBuffer currentBuffer;

    Deflater compresser;
    byte[] afterZip = new byte[ZipSize];
    AsyncLogging(String parent,String fileName){
        this.filePath = parent+fileName + AsyncLogging.fileMagicNumber;
        try {
            out = new FileOutputStream(new File(filePath),true);
            //File sss = new File(filePath);
            //out = new FileOutputStream(sss, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        lock  = new ReentrantLock();
        currentBuffer = ByteBuffer.allocate(blockingSize);
        compresser = new Deflater(Deflater.BEST_SPEED);
    }

    public void append(byte[] byteArray,int len) {
        lock.lock();
        if (currentBuffer.remaining() > len) {
            currentBuffer.put(byteArray);
        } else {
            compresser.setInput(currentBuffer.array(),0,currentBuffer.position());
            compresser.finish();
            int compressedDataLength = compresser.deflate(afterZip);
            try {
                out.write(int2byte(compressedDataLength));
                out.write(afterZip,0,compressedDataLength);
            } catch (IOException e) {
                e.printStackTrace();
            }
            compresser.reset();
            currentBuffer.clear();
            currentBuffer.put(byteArray);
        }
        lock.unlock();
    }
    //Inflater decompresser = new Inflater();
    public void signalFlush(){
        lock.lock();
        //filePosition += curPosition;
        try {
            compresser.setInput(currentBuffer.array(),0,currentBuffer.position());
            compresser.finish();
            int compressedDataLength = compresser.deflate(afterZip);
            /*
            decompresser.setInput(beforeZip, 0, compressedDataLength);
            try {
                int resultLength = decompresser.inflate(afterZip);
                System.out.println(resultLength);
            } catch (DataFormatException e) {
                e.printStackTrace();
            }
            */
            out.write(int2byte(compressedDataLength));
            out.write(afterZip,0,compressedDataLength);
            compresser.end();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        lock.unlock();
    }
    //variables for thread


    //加入这部分是因为byteBuffer的putInt方式与一般的putInt方式不一样，一个是大端，一个是小端。至于哪个是大端哪个是小端我就不知道了。。
    private static byte[] int2byte(int res) {
        byte[] targets = new byte[4];
        targets[0] = (byte) (res >>> 24);// 最低位
        targets[1] = (byte) ((res >> 16) & 0xff);// 次低位
        targets[2] = (byte) ((res >> 8) & 0xff);// 次高位
        targets[3] = (byte) (res & 0xff);// 最高位,无符号右移。
        return targets;
    }
    private void writeFile(){
        try {
            out.write(currentBuffer.array(),0,currentBuffer.position());
            out.flush();
            currentBuffer.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}