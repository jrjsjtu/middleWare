package io.openmessaging.demo;

import io.openmessaging.*;
import io.openmessaging.tester.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class DefaultPullConsumer implements PullConsumer {
    private static final int step = 20;
    private static Semaphore threadNumberControl;
    static{
        threadNumberControl = new Semaphore(step);
    }

    private KeyValue properties;
    private ArrayList<String> channelsList = new ArrayList<>();
    String parent;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        parent = properties.getString("STORE_PATH");
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    fileNode tmpFileNode;
    Message singleMessage;
    private boolean firstTime = true;
    @Override
    public Message poll() {
        //这里用来限制统一时刻运行的线程数
        //每一时刻只能运行step个线程
        if (firstTime){
            try {
                threadNumberControl.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            tmpFileNode = new fileNode(parent+channelsList.get(0));
            firstTime = false;
        }

        singleMessage = tmpFileNode.getOneMessage();
        if (singleMessage != null){
            return singleMessage;
        }else{
            if (channelsList.size() == 0){
                tmpFileNode = null;channelsList = null;
                threadNumberControl.release();
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
        if (hasQueueFile(queueName)){
            channelsList.add(queueName);
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
