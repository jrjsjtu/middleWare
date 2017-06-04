package io.openmessaging.demo;

import io.openmessaging.*;
import io.openmessaging.tester.Constants;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultPullConsumer implements PullConsumer {

    private KeyValue properties;
    String parent;
    private int consumerIndex;
    public BlockingQueue<BytesMessage> msgQueue = new LinkedBlockingDeque(1500);

    static public HashMap<String,ArrayList<DefaultPullConsumer>> interestList;
    static public ArrayList<DefaultPullConsumer> notifyList = new ArrayList<>();
    public static AtomicInteger remainThread = new AtomicInteger(10);
    private static AtomicInteger consumerNum = new AtomicInteger(0);

    private int interestNumber;
    static{
        interestList = new HashMap<>(500);
    }
    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        parent = properties.getString("STORE_PATH");
        consumerIndex = consumerNum.incrementAndGet();
    }

    @Override
    public KeyValue properties() {
        return properties;
    }


    BytesMessage tmpMessage;
    int a =0;
    @Override
    public Message poll() {
        try {
            tmpMessage = msgQueue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (tmpMessage.getBody().length == 1123){
            return  null;
        }
        return tmpMessage;
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
        ArrayList tmpArray;
        synchronized (interestList){
            notifyList.add(this);

            tmpArray = interestList.get(queueName);
            if (tmpArray == null){
                tmpArray = new ArrayList<>();
                interestList.put(queueName,tmpArray);
            }
            tmpArray.add(this);
            for (String tmpStr: topics){
                tmpArray = interestList.get(tmpStr);
                if (tmpArray == null){
                    tmpArray = new ArrayList<>();
                    interestList.put(tmpStr,tmpArray);
                }
                tmpArray.add(this);
            }
        }
        if (consumerIndex == 10){
            for (int i=0;i<10;i++){
                System.out.println("start read" + i);
                new fileNode(parent+i+DefaultProducer.magicNumber).start();
            }
        }
    }
}