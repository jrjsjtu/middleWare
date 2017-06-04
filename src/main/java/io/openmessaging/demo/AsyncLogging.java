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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.Deflater;

/**
 * Created by jrj on 17-5-22.
 */
public class AsyncLogging implements Runnable{
    private static final int blockingSize = 1024*1024*2;//4MB
    ByteBuffer currentBuffer;
    ByteBuffer nextBuffer;
    LinkedList<ByteBuffer> buffers_;
    boolean running_;
    String filePath;
    Lock lock;
    Condition condition;

    //因为多次观察就10个producer，所以也不用向之前一样做复杂的控制
    public static CountDownLatch endSignal = new CountDownLatch(10);
    AsyncLogging(String parent,String fileName){
        this.filePath = parent+fileName;
        running_ = true;

        lock  = new ReentrantLock();
        condition = lock.newCondition();
        currentBuffer = ByteBuffer.allocate(blockingSize);
        nextBuffer = ByteBuffer.allocate(blockingSize);
        buffers_ = new LinkedList();
        compresser = new Deflater(Deflater.BEST_SPEED);
    }


    public void signalFlush(){
        lock.lock();
        running_ = false;
        condition.signal();
        lock.unlock();
    }

    public void append(byte[] byteArray,int len){
        if (currentBuffer.remaining()>len){
            currentBuffer.put(byteArray);
        }else{
            lock.lock();
            zipBytebuffer();
            buffers_.add(currentBuffer);
            if (nextBuffer != null){
                currentBuffer = nextBuffer;
                nextBuffer = null;
            }else{
                currentBuffer = ByteBuffer.allocate(blockingSize);
            }
            currentBuffer.put(byteArray);
            condition.signal();
            lock.unlock();
        }
    }

    //variables for thread
    ByteBuffer newBuffer1 = ByteBuffer.allocate(blockingSize);
    ByteBuffer newBuffer2 = ByteBuffer.allocate(blockingSize);
    ByteBuffer tmpBuffer;
    LinkedList<ByteBuffer> tmp;
    LinkedList<ByteBuffer> buffersToWrite = new LinkedList();
    FileOutputStream out = null;

    Deflater compresser;

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
        exchangeBuffer();
        if (buffersToWrite.size() != 0){
            writeFile();
        }
        try {
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        endSignal.countDown();
    }

    private void exchangeBuffer(){
        //zipBytebuffer();
        buffers_.add(currentBuffer);
        currentBuffer = newBuffer1;
        newBuffer1 = null;
        tmp = buffersToWrite;buffersToWrite = buffers_;buffers_=tmp;//swap buffers_ and buffersToWrite
        if (nextBuffer == null){
            nextBuffer = newBuffer2;
            newBuffer2 = null;
        }
    }

    byte[] zipBuffer =  new byte[blockingSize];
    byte[] tmpZipBuffer;

    private void zipBytebuffer(){
        compresser.setInput(currentBuffer.array(),0,currentBuffer.position());
        compresser.finish();
        int compressedDataLength = compresser.deflate(zipBuffer);
        compresser.reset();
        //交换ByteBuffer的Array与ZipBuffer免去重新申请内存
        tmpZipBuffer = currentBuffer.array();
        currentBuffer = ByteBuffer.wrap(zipBuffer);
        zipBuffer = tmpZipBuffer;
        currentBuffer.position(compressedDataLength);
    }

    private void writeFile(){
        //System.out.println(buffersToWrite.size());
        try {
            for (int i=0;i<buffersToWrite.size();i++){
                tmpBuffer = buffersToWrite.get(i);
                out.write(int2byte(tmpBuffer.position()));
                out.write(tmpBuffer.array(),0,tmpBuffer.position());
            }
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