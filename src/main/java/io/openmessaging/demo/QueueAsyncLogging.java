package io.openmessaging.demo;

import io.openmessaging.demo.JRJSer.AbstractLogging;
import io.openmessaging.demo.JRJSer.ILogging;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by jrj on 17-6-2.
 */
public class QueueAsyncLogging  extends AbstractLogging {
    //use one thread to manage multiple files
    private static final int blockingSize = 1024*512;//Leave 512KB for QueueLogging
    ByteBuffer currentBuffer;
    ByteBuffer nextBuffer;
    LinkedList<ByteBuffer> buffers_;
    boolean running_;

    String filePath;

    Lock lock;
    Condition condition;

    QueueAsyncLogging(String parent,String fileName){
        this.filePath = parent+fileName + AbstractLogging.fileMagicNumber;
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
        running_ = false;
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

    public static AtomicInteger i = new AtomicInteger(0);
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


    //加入这部分是因为byteBuffer的putInt方式与一般的putInt方式不一样，一个是大端，一个是小端。至于哪个是大端哪个是小端我就不知道了。。
    private static byte[] int2byte(int res) {
        byte[] targets = new byte[4];
        targets[0] = (byte) (res >>> 24);// 最低位
        targets[1] = (byte) ((res >> 16) & 0xff);// 次低位
        targets[2] = (byte) ((res >> 8) & 0xff);// 次高位
        targets[3] = (byte) (res & 0xff);// 最高位,无符号右移。
        return targets;
    }
}
