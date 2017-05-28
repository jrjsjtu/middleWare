package io.openmessaging.demo;

import io.openmessaging.*;
import io.openmessaging.tester.Constants;
import io.openmessaging.tester.ConsumerTester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DefaultPullConsumer implements PullConsumer {

    static Logger logger = LoggerFactory.getLogger(ConsumerTester.class);
    private KeyValue properties;
    //通知队列
    ArrayList<BytesMessage> curArrayList = null;

    private ArrayList<fileNode> channelsList = new ArrayList<>();
    byte[] byte4int = new byte[4];
    byte[] byte4message = new byte[1024*1024*2];

    ByteBuffer intByteBuffer = ByteBuffer.wrap(byte4int);
    int topicNumber = 0;
    int cur_node = 0;
    String parent;
    class fileNode{
        long fileSize;
        long curPostion = 0;
        RandomAccessFile raf;
        public fileNode(String fileName){
            try {
                raf = new RandomAccessFile (fileName, "r");
                fileSize = raf.length();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public ByteBuffer getByteBuffer(){
            if (curPostion == fileSize){
                return null;
            }
            try {
                raf.read(byte4int);
                intByteBuffer = ByteBuffer.wrap(byte4int);
                int tmp = intByteBuffer.getInt();
                curPostion += (4+tmp);
                raf.read(byte4message,0,tmp);
                return ByteBuffer.wrap(byte4message,0,tmp);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
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

    ArrayList<BytesMessage> messagesArray = null;
    Iterator iter;
    @Override
    public Message poll() {
        if (messagesArray != null){
            BytesMessage bytesMessage = (BytesMessage) iter.next();
            if (!iter.hasNext()){
                messagesArray = null;
                iter = null;
            }
            return bytesMessage;
        }else{
            if (channelsList.size() == 0){
                return null;
            }else{
                int curSize = channelsList.size();
                ByteBuffer tmpBuffer = channelsList.get(cur_node%curSize).getByteBuffer();
                if (tmpBuffer == null){
                    channelsList.remove(cur_node%curSize);
                    return poll();
                }else{
                    cur_node ++;
                    messagesArray = getMessageList(tmpBuffer);
                    iter = messagesArray.iterator();
                    return poll();
                }
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
            if (defaultPullConsumer.poll() == null){
                break;
            }else{
                a ++;
            }
        }
        //System.out.println(a);
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
            channelsList.add(new fileNode(parent+queueName));
            topicNumber ++;
        }
        // 这个++是因为有一个topic存在的关系，把topic和queue都抽象成一个consumerFileManager
        for (String tmpStr: topics){
            channelsList.add(new fileNode(parent + tmpStr));
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

    private ArrayList<BytesMessage> getMessageList(ByteBuffer byteBuffer){
        ArrayList<BytesMessage> messagesArray = new ArrayList<>();
        int len;
        byte[] body;
        int strlen,vallen;byte[] tmpkey,tmpvalue;String key,valuestr;
        while (byteBuffer.hasRemaining()){
            len = byteBuffer.getInt();
            body = new byte[len];
            byteBuffer.get(body);
            DefaultBytesMessage message = new DefaultBytesMessage(body);
            while (true){
                char tmp = 'a';
                try{
                    tmp = byteBuffer.getChar();
                }catch (Exception e){
                    e.printStackTrace();
                }
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
    public void addBuffer(ArrayList<BytesMessage> messageList){
    }
}
