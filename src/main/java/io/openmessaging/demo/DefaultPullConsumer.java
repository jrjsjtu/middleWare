package io.openmessaging.demo;

import io.openmessaging.*;
import io.openmessaging.tester.Constants;
import io.openmessaging.tester.ConsumerTester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultPullConsumer implements PullConsumer {
    private static int step = 30;
    private static ArrayList<CountDownLatch> fuckList;
    private static AtomicInteger consumerIndex = new AtomicInteger(0);
    private int index;
    //static Logger logger = LoggerFactory.getLogger(ConsumerTester.class);
    private KeyValue properties;

    private ArrayList<String> channelsList = new ArrayList<>();

    int topicNumber = 0;
    String parent;
    static{
        fuckList = new ArrayList<>();
        fuckList.add(new CountDownLatch(step));
        fuckList.add(new CountDownLatch(step));
        fuckList.add(new CountDownLatch(step));
        fuckList.add(new CountDownLatch(step));
        fuckList.add(new CountDownLatch(step));
    }
    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        parent = properties.getString("STORE_PATH");
    }

    public int length = 0;
    @Override
    public KeyValue properties() {
        return properties;
    }

    boolean firstTime = true;
    fileNode tmpFileNode;
    Message singleMessage;
    @Override
    public Message poll() {
        //这里用来限制统一时刻运行的线程数
        if (firstTime){
            index = consumerIndex.getAndIncrement();
            if (index % step == 0){
                fuckList.add(new CountDownLatch(step));
            }
            if (index/step >= 1){
                try {
                    fuckList.get(index/step-1).await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            tmpFileNode = new fileNode(parent+channelsList.get(0));
            firstTime = false;
        }
        //到这里没一批次只能运行step个线程
        singleMessage = tmpFileNode.getOneMessage();
        if (singleMessage != null){
            return singleMessage;
        }else{
            if (channelsList.size() == 0){
                fuckList.get(index/step).countDown();
                tmpFileNode = null;channelsList = null;
                return null;
            }else{
                tmpFileNode.closeFileFD();
                channelsList.remove(0);
                if (channelsList.size() >0){
                    tmpFileNode = new fileNode(parent+channelsList.get(0));
                }
                return poll();
            }
        }
    }

    public static void main(String[] args){
        Class kvClass = null;
        KeyValue keyValue = null;
        try {
            kvClass = Class.forName("io.openmessaging.demo.DefaultKeyValue");
            keyValue = (KeyValue) kvClass.newInstance();
            keyValue.put("STORE_PATH", Constants.STORE_PATH);
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<String> topList= new ArrayList<>();
        topList.add(Constants.TOPIC_PRE + 1);
        DefaultPullConsumer defaultPullConsumer = new DefaultPullConsumer(keyValue);
        defaultPullConsumer.attachQueue(Constants.QUEUE_PRE+1,topList);
        int a = 0;
        while (true){
            BytesMessage bytesMessage = (BytesMessage) defaultPullConsumer.poll();
            if (bytesMessage == null){
                break;
            }else{
                //System.out.println(new String(bytesMessage.getBody()));
                a ++;
            }
        }
        System.out.println(a);
    }
    @Override
    public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void attachQueue(String queueName, Collection<String> topics) {
        topicNumber = topics.size();
        if (hasQueueFile(queueName)){
            channelsList.add(queueName);
            topicNumber ++;
        }
        // 这个++是因为有一个topic存在的关系，把topic和queue都抽象成一个consumerFileManager
        for (String tmpStr: topics){
            channelsList.add(tmpStr);
        }
    }

    private boolean hasQueueFile(String queueName){
        File file=new File(parent+queueName);
        if (file.exists()){
            return true;
        }else{
            return false;
        }
    }

}
