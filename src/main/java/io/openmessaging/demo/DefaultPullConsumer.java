package io.openmessaging.demo;

import io.openmessaging.*;
import io.openmessaging.tester.Constants;
import io.openmessaging.tester.ConsumerTester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DefaultPullConsumer implements PullConsumer {

    static Logger logger = LoggerFactory.getLogger(ConsumerTester.class);
    private KeyValue properties;
    //通知队列
    private static HashMap<String,ConsumerFileManager> fileMap = new HashMap<>();
    private BlockingQueue<ArrayList<BytesMessage>> noticeQueue = new ArrayBlockingQueue<ArrayList<BytesMessage>>(2,true);
    ArrayList<BytesMessage> curArrayList = null;
    Iterator iter = null;

    int topicNumber = 0;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }

    public int length = 0;
    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message poll() {
        if (iter == null) {
            try {
                while (true){
                    curArrayList = noticeQueue.take();
                    if (curArrayList.size() == 0){
                        topicNumber--;
                        if (topicNumber == 0){
                            return null;
                        }
                    }else{
                        break;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            iter = curArrayList.iterator();
        }
        Message tmp = null;
        try{
            tmp =  (Message) iter.next();
            if (!iter.hasNext()){
                iter = null;
                curArrayList = null;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        length ++;
        return tmp;
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
        ConsumerFileManager consumerFileManager;
        topicNumber = topics.size();
        if (hasQueueFile(queueName)){
            consumerFileManager = getFileManager(queueName);
            consumerFileManager.register(this);
            topicNumber ++;
        }
        // 这个++是因为有一个topic存在的关系，把topic和queue都抽象成一个consumerFileManager
        for (String tmpStr: topics){
            consumerFileManager = getFileManager(tmpStr);
            consumerFileManager.register(this);
        }
    }

    private boolean hasQueueFile(String queueName){
        File file=new File(Constants.STORE_PATH+queueName);
        if (file.exists()){
            return true;
        }else{
            return false;
        }
    }

    private ConsumerFileManager getFileManager(String fileName){
        ConsumerFileManager FileManager = fileMap.get(fileName);
        if (FileManager == null){
            synchronized (fileMap){
                FileManager = fileMap.get(fileName);
                if (FileManager ==null){
                    FileManager = new ConsumerFileManager(fileName);
                    fileMap.put(fileName,FileManager);//尽管synchronize的代价很大，但是只有在第一次创建topic或者queue的时候发生。仍然可以接受
                    new Thread(FileManager).start();
                }
            }
        }
        return FileManager;
    }

    public void addBuffer(ArrayList<BytesMessage> messageList){
        try {
            noticeQueue.put(messageList);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
