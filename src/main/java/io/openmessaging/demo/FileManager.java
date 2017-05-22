package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.demo.JRJSer.JByteBuffer;
import io.openmessaging.tester.Constants;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by JRJ on 2017/5/20.
 */
public class FileManager {
    private static final int queueLength = 100; //使用一个队列去缓存mappedBytebuffer，只是为了解决上一区块没写完就进入下一区块的问题.反正只是空引用不必过多担心内存不够
    private static final int blockSize = 1024*1024*1; //一个区块16mb
    private static final int cacheSize = 3;//用三个block作为缓冲区，可以认为当我写到第3个block时将第0个block置为null
    private static final int epochSize = queueLength*blockSize;//一次循环写了queueLength*blockSize的数据

    //File local variables
    private JByteBuffer[] cacheList;
    AtomicLong offset  = null;
    FileChannel fileChannel;
    RandomAccessFile aFile;
    int header = 0;

    FileManager(String fileName){
        offset = new AtomicLong(0);
        cacheList = new JByteBuffer[queueLength];
        try {
            aFile = new RandomAccessFile(Constants.STORE_PATH+"/"+fileName, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        cacheList[0] = new JByteBuffer(blockSize);
    }

    public void send0(Message message,long end){
        byte[] tmp = ((DefaultBytesMessage) message).getByteArray();
        long start = end-tmp.length;
        int startIdndex = (int)(start/blockSize%queueLength);
        int endIndex = (int)(end/blockSize%queueLength);
        int inblockIndex = (int)(start%blockSize);
        if (startIdndex == endIndex){
            if(cacheList[startIdndex] == null){
                //这里为null的情况我认为很少发生，所以用synchronzied也没有关系
                if (start % blockSize == 0){//1.写数据的头部正好在下一个区块的头部,在这边创建新快
                    synchronized (cacheList){
                        header = startIdndex;
                        cacheList[startIdndex] = new JByteBuffer(blockSize);
                        cacheList[startIdndex].put(tmp,inblockIndex,tmp.length);
                        int curIndex = (endIndex-cacheSize+queueLength)%queueLength;
                        if (cacheList[curIndex] != null){
                            try {
                                aFile.write(cacheList[curIndex].array());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            cacheList[curIndex] = null;
                        }
                    }
                }else {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    send0(message,end);
                }
            }else{
                cacheList[startIdndex].put(tmp,inblockIndex,tmp.length); // 大部分情况send函数中这个判断中只会执行这一句。但这里还是可能出现空指针的情况。要是出bug一定要注意这里
            }
        }else{
            long newMap = end / blockSize * blockSize;
            //这个方法最主要的困难就是，当map了一个新的buffer时，没有办法保证使用上一个buffer的producer都写完了，因为其他线程不受控制。
            //我的意见是上一个buffer先不回收，等到大概率某一个buffer写完了再回收那个buffer，之后用try catch写，如果真的有某个线程些了那个空buffer，再重新map出来。
            //用那个mapBytebuffer[]的原因是防止上一个epoch的数据写到下一个epoch去，本来数组就是10个引用而已，但可以起到160mb缓冲的效果。
            byte[] part1,part2;int i=0;
            part1 = new byte[blockSize-inblockIndex];
            part2 = new byte[tmp.length - (blockSize-inblockIndex)];
            for(i=0;i<part1.length;i++){
                part1[i] = tmp[i];
            }
            for(int j=0;i<part2.length;i++,j++){
                part2[j] = tmp[i];
            }
            cacheList[startIdndex].put(part1,inblockIndex,part1.length);
            synchronized (cacheList) {
                int curIndex = (endIndex-cacheSize+queueLength)%queueLength;
                if (cacheList[curIndex] != null){
                    try {
                        aFile.write(cacheList[curIndex].array());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    cacheList[curIndex] = null;
                }
                cacheList[endIndex] = new JByteBuffer(blockSize);
            }
            cacheList[startIdndex].put(part2,0,part2.length);
        }
    }
    public void send(Message message){
        byte[] tmp = ((DefaultBytesMessage) message).getByteArray();
        long end = offset.addAndGet(tmp.length);//这句自增是这段程序的核心，用来保证消息的写入顺序
        send0(message,end);
    }
}
