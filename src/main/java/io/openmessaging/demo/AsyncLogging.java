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
    private static final int blockingSize = 1024*1024*2;//2MB
    public static final int fileMagicNumber = 1156217149;
    private static final int fileMapSize =32*1024*1024;
    FileOutputStream out = null;

    String filePath;
    Lock lock;
    ByteBuffer currentBuffer;

    MappedByteBuffer mbb ;
    FileChannel fc;
    RandomAccessFile raf;

    long curPosition;
    long filePosition;
    AsyncLogging(String parent,String fileName){
        this.filePath = parent+fileName + AsyncLogging.fileMagicNumber;
        try {
            raf = new RandomAccessFile(this.filePath,"rw");
            fc = raf.getChannel();
            //File sss = new File(filePath);
            //out = new FileOutputStream(sss, true);
            mbb = fc.map(FileChannel.MapMode.READ_WRITE,0,fileMapSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
        lock  = new ReentrantLock();
        currentBuffer = ByteBuffer.allocate(blockingSize);
    }

    public void append(byte[] byteArray,int len) {
        lock.lock();
        if (currentBuffer.remaining() > len) {
            currentBuffer.put(byteArray);
        } else {
            if (curPosition + currentBuffer.position() < fileMapSize) {
                mbb.put(currentBuffer.array(), 0, currentBuffer.position());
                filePosition += currentBuffer.position();
                curPosition += currentBuffer.position();
            } else {
                try {
                    mbb = fc.map(FileChannel.MapMode.READ_WRITE, filePosition, fileMapSize);
                    mbb.put(currentBuffer.array(), 0, currentBuffer.position());
                    filePosition += currentBuffer.position();
                    curPosition = currentBuffer.position();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            currentBuffer.clear();
            currentBuffer.put(byteArray);
        }
        lock.unlock();
    }

    public void signalFlush(){
        lock.lock();
        //filePosition += curPosition;
        try {
            if (curPosition + currentBuffer.position() < fileMapSize) {
                mbb.put(currentBuffer.array(), 0, currentBuffer.position());
                filePosition += currentBuffer.position();
            } else {
                try {
                    mbb = fc.map(FileChannel.MapMode.READ_WRITE, filePosition, fileMapSize);
                    mbb.put(currentBuffer.array(), 0, currentBuffer.position());
                    filePosition += currentBuffer.position();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
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