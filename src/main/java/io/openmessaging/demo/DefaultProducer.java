package io.openmessaging.demo;

import io.openmessaging.*;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultProducer implements Producer {
    private MessageFactory messageFactory = new DefaultMessageFactory();
    static Long startTime;
    static{
        //获得进程开始时间，防止有的producer线程过早结束，而使得程序过早结束
        startTime = System.currentTimeMillis();
    }
    public static HashMap<String,AsyncLogging> fileMap = new HashMap();

    private KeyValue properties;
    String parent;
    boolean isStart = true;
    private static AtomicInteger producerNumber = new AtomicInteger(0);
    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
        producerNumber.getAndIncrement();
        parent = properties.getString("STORE_PATH");
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

    @Override
    public void send(Message message) {
        if(isStart){
            if (message == null) throw new ClientOMSException("Message should not be null");
            String topic = message.headers().getString(MessageHeader.TOPIC);
            String queue = message.headers().getString(MessageHeader.QUEUE);
            if ((topic == null && queue == null) || (topic != null && queue != null)) {
                throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
            }

            String fileName = null;
            if (topic != null) {
                fileName = topic;
            } else {
                fileName = queue;
            }
            AsyncLogging fileManager = getFileManager(fileName);
            byte[] tmp = ((DefaultBytesMessage)message).getByteArray();
            fileManager.append(tmp,tmp.length);
        }else{
            return;
        }
    }

    private AsyncLogging getFileManager(String fileName){
        AsyncLogging fileLogger = fileMap.get(fileName);
        if (fileLogger == null){
            synchronized (fileMap){
                fileLogger = fileMap.get(fileName);
                if (fileLogger ==null){
                    fileLogger = new AsyncLogging(parent,fileName);
                    fileMap.put(fileName,fileLogger);//尽管synchronize的代价很大，但是只有在第一次创建topic或者queue的时候发生。仍然可以接受
                    new Thread(fileLogger).start();
                }
            }
        }
        return fileLogger;
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
        isStart = false;
        int  aaa = producerNumber.decrementAndGet();
        //System.out.println(aaa);
        if (aaa == 0){
            /*
            if ((System.currentTimeMillis() - startTime)<110000l){
                //return;
                //我们的程序起码要110S结束，比100S还要早结束什么不存在的。
            }
            synchronized (fileMap){
                //保险起见，还是synchronize一下。
                Iterator iter = fileMap.entrySet().iterator();
                while (iter.hasNext()){
                    Map.Entry entry = (Map.Entry) iter.next();
                    AsyncLogging val = (AsyncLogging) entry.getValue();
                    val.signalFlush();
                }
                //fileMap = new HashMap();
            }
            */
            try {
                //这里留一点时间给最后持久化，我观察到比赛机器上kill -9总是失败额。不知道阿里那边怎么回事。
                Thread.sleep(10000);
                System.out.println("here we exit");
                if (producerNumber.get() == 0){
                    System.exit(0);
                }
                //我也绝望啊，要这么靠运气自己结束自己
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
