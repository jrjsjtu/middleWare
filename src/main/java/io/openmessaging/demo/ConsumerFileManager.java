package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import io.openmessaging.tester.Constants;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by jrj on 17-5-23.
 */
public class ConsumerFileManager implements Runnable {
    TreeMap<Integer,ArrayList<PullConsumer>> treeMap; //管理每一个consumer读到了哪里？并且top节点一定是最小的

    ArrayList<PullConsumer> header;
    ArrayList<PullConsumer> tailer;
    ArrayList<blockNode> fileIndex;

    long fileSize;
    Condition condition;
    Lock lock;
    Long headOffset;

    FileChannel fc = null;

    class blockNode{
        public long blockOffset;
        public int blockSize;
        blockNode(long blockOffset,int blockSize){this.blockOffset = blockOffset;this.blockSize = blockSize;}
    }

    ConsumerFileManager(String fileName){
        fileIndex = new ArrayList<>();

        lock = new ReentrantLock();
        condition = lock.newCondition();

        header = new ArrayList<>();
        tailer = new ArrayList<>();
        treeMap = new TreeMap<>();
        try{
            fc = new RandomAccessFile(Constants.STORE_PATH+fileName, "r").getChannel();
            fileSize = fc.size();
            ByteBuffer firstInt = ByteBuffer.allocate(4);
            fc.read(firstInt);
            firstInt.flip();
            int tmpInt = firstInt.getInt();
            fileIndex.add(new blockNode(4l,tmpInt));
            headOffset = 4l + tmpInt;
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    void register(PullConsumer pullConsumer){
        synchronized (treeMap){
            if (treeMap.get(0) != null){
                treeMap.get(0).add(pullConsumer);
            }else{
                ArrayList<PullConsumer> tmpArrayList = new ArrayList<>();
                tmpArrayList.add(pullConsumer);
                treeMap.put(0,tmpArrayList);
            }
        }
    }

    @Override
    public void run() {
        int cur_node;
        ArrayList<PullConsumer> pullArray;
        ByteBuffer byteBuffer4Block = null;
        blockNode tmpNode;
        while(true){
            synchronized (treeMap){
                if (treeMap.size() == 0){
                    try {
                        treeMap.wait(300);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }else{
                    cur_node = treeMap.firstKey();
                    pullArray = treeMap.firstEntry().getValue();
                    treeMap.remove(cur_node++);//第一个节点中的链表是将要发送的节点。
                    if (treeMap.get(cur_node) != null){
                        treeMap.get(cur_node).addAll(pullArray);//合并两节链表
                    }else{
                        treeMap.put(cur_node,pullArray);
                    }
                    cur_node --;
                }
            }
            int arraySize = fileIndex.size();//cur_node == 0 时,fileIndex.size()=1应该读取 fileIndex.get(0).offset位置
            if (cur_node<arraySize){
                tmpNode = fileIndex.get(cur_node);
                byteBuffer4Block = ByteBuffer.allocate(tmpNode.blockSize);
                try {
                    fc = fc.position(tmpNode.blockOffset);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else{
                tmpNode = fileIndex.get(cur_node-1);//这里的机制保证cur_node-1处一定有节点
                int tmpInt = 0;
                try {
                    Long curPostion = tmpNode.blockOffset+tmpNode.blockSize;
                    if(curPostion == fileSize){
                        treeMap.remove(cur_node+1);
                        for (int i=0;i<pullArray.size();i++){
                            //用这个空的链表表示这个topic的信息结束了。
                            ((DefaultPullConsumer)(pullArray.get(i))).addBuffer(new ArrayList<>());
                        }
                        continue;
                    }
                    fc = fc.position(curPostion);
                    ByteBuffer firstInt = ByteBuffer.allocate(4);
                    fc.read(firstInt);
                    firstInt.flip();
                    tmpInt = firstInt.getInt();
                    fileIndex.add(new blockNode(curPostion+4,tmpInt));
                    byteBuffer4Block = ByteBuffer.allocate(tmpInt);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            ArrayList<BytesMessage> result = null;
            try {
                fc.read(byteBuffer4Block);
                result = getMessageList(byteBuffer4Block);
            } catch (Exception e) {
                e.printStackTrace();
            }
            for (int i=0;i<pullArray.size();i++){
                ((DefaultPullConsumer)(pullArray.get(i))).addBuffer(result);
            }
        }
    }

    public static void main(String[] args){
        ConsumerFileManager consumerFileManager = new ConsumerFileManager("TOPIC_1");
        DefaultPullConsumer defaultPullConsumer = new DefaultPullConsumer(new DefaultKeyValue());
        new Thread(consumerFileManager).start();
        consumerFileManager.register(defaultPullConsumer);
        BytesMessage bytesMessage;
        while (true){
            bytesMessage = (BytesMessage) defaultPullConsumer.poll();
            System.out.println(new String(bytesMessage.getBody()));
        }
    }

    private static ArrayList<BytesMessage> getMessageList(ByteBuffer byteBuffer){
        ArrayList<BytesMessage> messagesArray = new ArrayList<>();
        byteBuffer.flip();
        int len;
        byte[] body;
        int strlen,vallen;byte[] tmpkey,tmpvalue;String key,valuestr;
        while (byteBuffer.hasRemaining()){
            len = byteBuffer.getInt();
            body = new byte[len];
            byteBuffer.get(body);
            DefaultBytesMessage message = new DefaultBytesMessage(body);
            while (true){
                char tmp = byteBuffer.getChar();
                if (tmp == ' ') break;
                switch (tmp){
                    case '1':
                        int headerInt = byteBuffer.getInt();
                        strlen = byteBuffer.getInt();
                        tmpkey = new byte[strlen];
                        byteBuffer.get(tmpkey);
                        key = new String(tmpkey);
                        message.putHeaders(key,headerInt);break;
                    case '2':
                        long headerLong = byteBuffer.getLong();
                        strlen = byteBuffer.getInt();
                        tmpkey = new byte[strlen];
                        byteBuffer.get(tmpkey);
                        key = new String(tmpkey);
                        message.putHeaders(key,headerLong);break;
                    case '3':
                        double headerDouble = byteBuffer.getDouble();
                        strlen = byteBuffer.getInt();
                        tmpkey = new byte[strlen];
                        byteBuffer.get(tmpkey);
                        key = new String(tmpkey);
                        message.putHeaders(key,headerDouble);break;
                    case '4':
                        strlen = byteBuffer.getInt();
                        tmpvalue = new byte[strlen];
                        byteBuffer.get(tmpvalue);
                        valuestr = new String(tmpvalue);
                        strlen = byteBuffer.getInt();
                        tmpkey = new byte[strlen];
                        byteBuffer.get(tmpkey);
                        key = new String(tmpkey);
                        message.putHeaders(key,valuestr);break;
                }
            }

            while (true){
                char tmp = byteBuffer.getChar();
                if (tmp == ' ') break;
                switch (tmp){
                    case '1':
                        int headerInt = byteBuffer.getInt();
                        strlen = byteBuffer.getInt();
                        tmpkey = new byte[strlen];
                        byteBuffer.get(tmpkey);
                        key = new String(tmpkey);
                        message.putProperties(key,headerInt);break;
                    case '2':
                        long headerLong = byteBuffer.getLong();
                        strlen = byteBuffer.getInt();
                        tmpkey = new byte[strlen];
                        byteBuffer.get(tmpkey);
                        key = new String(tmpkey);
                        message.putProperties(key,headerLong);break;
                    case '3':
                        double headerDouble = byteBuffer.getDouble();
                        strlen = byteBuffer.getInt();
                        tmpkey = new byte[strlen];
                        byteBuffer.get(tmpkey);
                        key = new String(tmpkey);
                        message.putProperties(key,headerDouble);break;
                    case '4':
                        strlen = byteBuffer.getInt();
                        tmpvalue = new byte[strlen];
                        byteBuffer.get(tmpvalue);
                        valuestr = new String(tmpvalue);
                        strlen = byteBuffer.getInt();
                        tmpkey = new byte[strlen];
                        byteBuffer.get(tmpkey);
                        key = new String(tmpkey);
                        message.putProperties(key,valuestr);break;
                }
            }
            messagesArray.add(message);
        }
        return messagesArray;
    }
}