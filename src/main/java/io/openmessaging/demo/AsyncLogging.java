package io.openmessaging.demo;

import io.openmessaging.tester.Constants;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by jrj on 17-5-22.
 */
public class AsyncLogging{
    //use one thread to manage multiple files
    public static CountDownLatch endSignal;
    private static final int blockingSize = 1024*1024*4;//2MB
    public static final int fileMagicNumber = 11217149;
    FileOutputStream out = null;

    String filePath;
    Lock lock;
    ByteBuffer currentBuffer;
    AsyncLogging(String parent,String fileName){
        this.filePath = parent+fileName + AsyncLogging.fileMagicNumber;
        try {
            File sss = new File(filePath);
            out = new FileOutputStream(sss, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        lock  = new ReentrantLock();
        currentBuffer = ByteBuffer.allocate(blockingSize);
    }

    public void append(byte[] byteArray,int len){
        lock.lock();
        if (currentBuffer.remaining()>len){
            currentBuffer.put(byteArray);
        }else{
            writeFile();
            currentBuffer.put(byteArray);
        }
        lock.unlock();
    }

    public void signalFlush(){
        lock.lock();
        writeFile();
        try {
            out.close();
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