package io.openmessaging.demo;

import io.openmessaging.*;

public class DefaultProducer implements Producer {
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private MessageStore messageStore = null;
    public static HashMap<String,AsyncLogging> fileMap = new HashMap();
    private KeyValue properties;

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
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
    }

    private AsyncLogging getFileManager(String fileName){
        AsyncLogging fileLogger = fileMap.get(fileName);
        if (fileLogger == null){
            synchronized (fileMap){
                fileLogger = fileMap.get(fileName);
                if (fileLogger ==null){
                    fileLogger = new AsyncLogging(fileName);
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



    }

}
