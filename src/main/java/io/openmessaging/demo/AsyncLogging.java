package io.openmessaging.demo;

import io.openmessaging.tester.Constants;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by jrj on 17-5-22.
 */
public class AsyncLogging{
    //use one thread to manage multiple files
    private static final int blockingSize = 1024*1024*1;//2MB
    public static final int fileMagicNumber = 1156217149;
    private static final int fileMapSize = 64*1024*1024;

    String filePath;
    Lock lock;
    ByteBuffer currentBuffer;

    MappedByteBuffer mbb ;
    FileChannel fc;
    RandomAccessFile raf;

    long curPosition;
    long filePosition;
    FileOutputStream out;
    AsyncLogging(String parent,String fileName){
        this.filePath = parent+fileName + AsyncLogging.fileMagicNumber;
        try {
            //raf = new RandomAccessFile(this.filePath,"rw");
            //fc = raf.getChannel();
            out = new FileOutputStream(new File(filePath), true);
            fc = out.getChannel();
            //File sss = new File(filePath);
            //out = new FileOutputStream(sss, true);
            mbb = fc.map(FileChannel.MapMode.READ_WRITE,0,fileMapSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
        lock  = new ReentrantLock();
        currentBuffer = ByteBuffer.allocate(blockingSize);
    }

    public void append(byte[] byteArray,int len){
        lock.lock();
        if (curPosition+len<fileMapSize){
            curPosition+=len;
            mbb.put(byteArray);
        }else{
            filePosition += curPosition;
            curPosition = 0;
            try {
                mbb = fc.map(FileChannel.MapMode.READ_WRITE,filePosition,fileMapSize);
            } catch (IOException e) {
                e.printStackTrace();
            }
            mbb.put(byteArray);
        }
        lock.unlock();
    }

    public void signalFlush(){
        lock.lock();
        filePosition += curPosition;
        try {
            raf.setLength(filePosition);
            fc.close();
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        lock.unlock();
    }
    //variables for thread


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