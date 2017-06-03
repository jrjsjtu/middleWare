package io.openmessaging.demo;

import io.openmessaging.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultProducer implements Producer {
    private MessageFactory messageFactory = new DefaultMessageFactory();
    public static final String magicNumber = "1232434";

    private static AtomicInteger producerNum= new AtomicInteger(0);
    FileOutputStream out;
    int producerIndex;
    private KeyValue properties;
    String parent;
    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
        //System.out.println("start producer index: " + producerNumber.getAndIncrement());
        //producerNumber.getAndIncrement();
        parent = properties.getString("STORE_PATH");
        //nameFileMap = new HashMap<>(210/3*4);
        producerIndex = producerNum.getAndIncrement();
        try {
            out = new FileOutputStream(new File(parent+producerIndex+magicNumber),true);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        //loadFactor is 0.75
        //messageStore = MessageStore.getInstance(properties.getString("STORE_PATH"));
    }


    @Override
    public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        return messageFactory.createBytesMessageToTopic(topic, body);
    }

    @Override
    public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        return messageFactory.createBytesMessageToQueue(queue, body);
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public KeyValue properties() {
        return properties;
    }
    private static final int blockingSize = 1024*1024*4;
    ByteBuffer byteBuffer = ByteBuffer.allocate(blockingSize);
    byte[] tmp;
    @Override
    public void send(Message message) {
        tmp = ((DefaultBytesMessage)message).getByteArray();
        if (byteBuffer.remaining()>tmp.length){
            byteBuffer.put(tmp);
        }else{
            try {
                out.write(byteBuffer.array(),0,byteBuffer.position());
                out.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            byteBuffer.clear();
            byteBuffer.put(tmp);
        }
    }

    @Override
    public void send(Message message, KeyValue properties) {
        DefaultBytesMessage bytesMessage = (DefaultBytesMessage) message;
        bytesMessage.setProperties(properties);
        send(bytesMessage);
    }

    @Override
    public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void flush() {
        try {
            out.write(byteBuffer.array(),0,byteBuffer.position());
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}