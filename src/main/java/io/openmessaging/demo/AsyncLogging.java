package io.openmessaging.demo;

import io.openmessaging.tester.Constants;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by jrj on 17-5-22.
 */
public class AsyncLogging implements Runnable{
    private static final int blockingSize = 1024*1024*2-4;//4MB
    ByteBuffer currentBuffer;
    ByteBuffer nextBuffer;
    LinkedList<ByteBuffer> buffers_;
    boolean running_;

    String filePath;

    Lock lock;
    Condition condition;

    AsyncLogging(String parent,String fileName){
        this.filePath = parent+fileName;
        running_ = true;

        lock  = new ReentrantLock();
        condition = lock.newCondition();
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
            condition.signal();
        }
        lock.unlock();
    }

    public void signalFlush(){
        lock.lock();
        condition.signal();
        lock.unlock();
    }
    //variables for thread
    ByteBuffer newBuffer1 = ByteBuffer.allocate(blockingSize);
    ByteBuffer newBuffer2 = ByteBuffer.allocate(blockingSize);
    ByteBuffer tmpBuffer;
    LinkedList<ByteBuffer> tmp;
    LinkedList<ByteBuffer> buffersToWrite = new LinkedList();
    FileOutputStream out = null;
    @Override
    public void run() {
        try {
            out = new FileOutputStream(new File(filePath), true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        while (running_){
            //start of synchronize
            lock.lock();
            if (buffers_.size() == 0){
                try {
                    //反正最后强制flush也不用三秒刷新一次了。。
                    //condition.await(3000, TimeUnit.MILLISECONDS);
                    condition.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            exchangeBuffer();
            lock.unlock();
            //End of synchronize
            if (buffersToWrite.size() == 0){continue;}
            writeFile();
        }
        try {
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void exchangeBuffer(){
        buffers_.add(currentBuffer);
        currentBuffer = newBuffer1;
        newBuffer1 = null;
        tmp = buffersToWrite;buffersToWrite = buffers_;buffers_=tmp;//swap buffers_ and buffersToWrite
        if (nextBuffer == null){
            nextBuffer = newBuffer2;
            newBuffer2 = null;
        }
    }
    private void writeFile(){
        try {
            for (int i=0;i<buffersToWrite.size();i++){
                tmpBuffer = buffersToWrite.get(i);
                out.write(int2byte(tmpBuffer.position()));
                out.write(tmpBuffer.array(),0,tmpBuffer.position());
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

    private static byte[] int2byte(int res) {
        byte[] targets = new byte[4];
        targets[0] = (byte) (res >>> 24);// 最低位
        targets[1] = (byte) ((res >> 16) & 0xff);// 次低位
        targets[2] = (byte) ((res >> 8) & 0xff);// 次高位
        targets[3] = (byte) (res & 0xff);// 最高位,无符号右移。
        return targets;
    }
}
