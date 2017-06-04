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
import java.util.zip.Deflater;

public class DefaultProducer implements Producer {
    private MessageFactory messageFactory = new DefaultMessageFactory();
    public static final String magicNumber = "2sa2324d3jj4";

    private static AtomicInteger producerNum= new AtomicInteger(0);
    FileOutputStream out;
    int producerIndex;
    private KeyValue properties;
    String parent;
    Deflater compresser;
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
        compresser = new Deflater(Deflater.BEST_SPEED);
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
    private static final int blockingSize = 1024*1024/2;
    ByteBuffer byteBuffer = ByteBuffer.allocate(blockingSize);
    byte[] tmp;
    byte[] zipBuffer =  new byte[blockingSize];
    @Override
    public void send(Message message) {
        tmp = ((DefaultBytesMessage)message).getByteArray();
        if (byteBuffer.remaining()>tmp.length){
            byteBuffer.put(tmp);
        }else{
            try {
                compresser.setInput(byteBuffer.array(),0,byteBuffer.position());
                compresser.finish();
                int compressedDataLength = compresser.deflate(zipBuffer);
                compresser.reset();
                out.write(int2byte(compressedDataLength));
                out.write(zipBuffer,0,compressedDataLength);
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
            compresser.setInput(byteBuffer.array(),0,byteBuffer.position());
            compresser.finish();
            int compressedDataLength = compresser.deflate(zipBuffer);
            compresser.reset();
            out.write(int2byte(compressedDataLength));
            out.write(zipBuffer,0,compressedDataLength);
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
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