package io.openmessaging.demo;

import io.openmessaging.tester.Constants;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by jrj on 17-5-22.
 */
public class AsyncLogging implements Runnable{
    private static final int blockingSize = 1024*1024*4;//4MB
    ByteBuffer currentBuffer;
    ByteBuffer nextBuffer;
    LinkedList<ByteBuffer> buffers_;
    boolean running_;

    String filePath;

    Integer intLock = new Integer(0);//this is used as condition variable.
    Lock lock;

    AsyncLogging(String fileName){
        this.filePath = Constants.STORE_PATH+fileName;
        running_ = true;
        lock  = new ReentrantLock();
        currentBuffer = ByteBuffer.allocate(blockingSize);
        nextBuffer = ByteBuffer.allocate(blockingSize);
        buffers_ = new LinkedList();
    }

    public void append(byte[] byteArray,int len){
        lock.lock();
        if (currentBuffer.remaining()>len){
            currentBuffer.put(byteArray);
        }else{
            buffers_.add(currentBuffer);
            if (nextBuffer != null){
                currentBuffer = nextBuffer;
                nextBuffer = null;
            }else{
                currentBuffer = ByteBuffer.allocate(blockingSize);
            }
            currentBuffer.put(byteArray);
            synchronized (intLock){intLock.notify();}
        }
        lock.unlock();
    }

    @Override
    public void run() {
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(new File(filePath), true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        ByteBuffer newBuffer1 = ByteBuffer.allocate(blockingSize);
        ByteBuffer newBuffer2 = ByteBuffer.allocate(blockingSize);
        ByteBuffer tmpBuffer;
        LinkedList<ByteBuffer> tmp;
        LinkedList<ByteBuffer> buffersToWrite = new LinkedList();
        while (running_){
            //start of synchronize
            lock.lock();
            if (buffers_.size() == 0){
                lock.unlock();
                synchronized (intLock){
                    try {intLock.wait(3000);} catch (InterruptedException e) {e.printStackTrace();}
                }
                lock.lock();
            }
            buffers_.add(currentBuffer);
            currentBuffer = newBuffer1;
            newBuffer1 = null;
            tmp = buffersToWrite;buffersToWrite = buffers_;buffers_=tmp;//swap buffers_ and buffersToWrite
            if (nextBuffer == null){
                nextBuffer = newBuffer2;
                newBuffer2 = null;
            }
            lock.unlock();
            //End of synchronize
            if (buffersToWrite.size() == 0){continue;}
            try {
                for (int i=0;i<buffersToWrite.size();i++){
                    out.write(buffersToWrite.get(i).array(),0,buffersToWrite.get(i).position());
                }
                out.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (newBuffer1 == null){
                newBuffer1 = buffersToWrite.pop();
                newBuffer1.clear();
            }

            if (newBuffer2 == null){
                newBuffer2 = buffersToWrite.pop();
                newBuffer2.clear();
            }
            buffersToWrite.clear();
        }
        try {
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
