package io.openmessaging.demo;

import io.openmessaging.tester.Constants;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by jrj on 17-5-22.
 */
public class AsyncLogging implements Runnable{
    //use one thread to manage multiple files
    public static CountDownLatch endSignal;
    private static final int blockingSize = 1024*1024*2;//2MB
    public static final int fileMagicNumber = 1127149;

    boolean running_;

    String filePath;

    Lock lock;
    Condition condition;
    Condition condition4Append;

    ArrayList<ByteBuffer> producerBufferList,fileBufferList,BackUpList,tmp;
    ByteBuffer currentBuffer;
    AsyncLogging(String parent,String fileName){
        this.filePath = parent+fileName + AsyncLogging.fileMagicNumber;
        running_ = true;

        lock  = new ReentrantLock();
        condition = lock.newCondition();
        condition4Append = lock.newCondition();

        producerBufferList = new ArrayList<>();
        fileBufferList = new ArrayList<>();
        BackUpList = new ArrayList<>();

        currentBuffer = ByteBuffer.allocate(blockingSize);
        producerBufferList.add(currentBuffer);
        producerBufferList.add(ByteBuffer.allocate(blockingSize));
        //producerBufferList.add(ByteBuffer.allocate(blockingSize));
    }

    public void append(byte[] byteArray,int len){
        lock.lock();
        if (producerBufferList.size()>0){
            currentBuffer = producerBufferList.get(0);
            if (currentBuffer.remaining()>len){
                currentBuffer.put(byteArray);
            }else{
                BackUpList.add(currentBuffer);
                producerBufferList.remove(0);
                condition.signal();
            }
        }else{
            try {
                condition4Append.await();
                lock.unlock();
                append(byteArray,len);
                return;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        lock.unlock();
    }

    public void signalFlush(){
        lock.lock();
        running_ = false;
        condition.signal();
        lock.unlock();
    }
    //variables for thread
    FileOutputStream out = null;

    @Override
    public void run() {
        try {
            File sss = new File(filePath);
            out = new FileOutputStream(sss, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        while (running_){
            //start of synchronize
            lock.lock();
            try {
                condition.await();
                for (ByteBuffer byteBuffer:BackUpList){
                    fileBufferList.add(byteBuffer);
                }
                BackUpList.clear();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            lock.unlock();
            //End of synchronize

            writeFile();

            lock.lock();

            for (ByteBuffer byteBuffer:fileBufferList){
                producerBufferList.add(byteBuffer);
            }
            fileBufferList.clear();
            condition4Append.signal();
            lock.unlock();
        }

        try {
            for (ByteBuffer byteBuffer: producerBufferList){
                out.write(byteBuffer.array(),0,byteBuffer.position());
            }
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        endSignal.countDown();
    }

    private void writeFile(){
        try{
            for (ByteBuffer byteBuffer:fileBufferList){
                out.write(byteBuffer.array(),0,byteBuffer.position());
                byteBuffer.clear();
            }
            out.flush();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}