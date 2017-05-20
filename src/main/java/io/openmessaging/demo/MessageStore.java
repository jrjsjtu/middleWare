package io.openmessaging.demo;

import io.openmessaging.Message;

import java.util.Collection;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 负责管理Message的存放以及获取
 */
public class MessageStore {

    private static final int MAXIMUM_POOL_SIZE = 128;

    private static final ThreadFactory sThreadFactory = new ThreadFactory() {
        private final AtomicInteger mCount = new AtomicInteger(1);

        public Thread newThread(Runnable r) {
            return new Thread(r, "MessageStore#" + mCount.getAndIncrement());
        }
    };

    private static MessageStore INSTANCE = null;

    //消息存储目录
    private String parent;

    private ExecutorService executor;

    private MessageStore(String parent) {
        this.parent = parent;
        executor = Executors.newFixedThreadPool(MAXIMUM_POOL_SIZE, sThreadFactory);
    }

    public static MessageStore getInstance(String parent) {

        if (INSTANCE == null) {
            synchronized (MessageStore.class) {
                if (INSTANCE == null)
                    INSTANCE = new MessageStore(parent);
            }
        }

        return INSTANCE;
    }

    //关联topic或queue与BlockingQueue
    private Map<String, TopicLinkedBlockingQueue<Message>> consumeMessageBuckets = new ConcurrentHashMap<>();

    /**
     * 从队列中获取Message
     *
     * @param queue
     * @return
     */
    public Message pullMessage(String queue) throws InterruptedException {

        TopicLinkedBlockingQueue<Message> queueBlockingQueue = consumeMessageBuckets.get(queue);
        Message message = queueBlockingQueue.poll();
        if (message != null)
            return message;
        return null;

    }

    public Message pullMessage(String bucket, int index) {

        TopicLinkedBlockingQueue<Message> topicBlockingQueue = consumeMessageBuckets.get(bucket);
        Message message = null;

        try {
            message = topicBlockingQueue.take(index);
        } catch (InterruptedException e) {
            e.printStackTrace();
            message = null;
        }


        return message;
    }


    public void registerConsumers(DefaultPullConsumer consumer, String queueName, Collection<String> topics) {

        TopicLinkedBlockingQueue<Message> queueBlockingQueue = consumeMessageBuckets.get(queueName);
        if (queueBlockingQueue == null) {
            queueBlockingQueue = new TopicLinkedBlockingQueue<>(1024);
            consumeMessageBuckets.put(queueName, queueBlockingQueue);
            ReadQueueTask task = new ReadQueueTask(parent, queueName, queueBlockingQueue);
            task.registerConsumer(consumer);
            executor.execute(task);
        }

        TopicLinkedBlockingQueue<Message> topicBlockingQueue = null;
        for (String bucket : topics) {
            topicBlockingQueue = consumeMessageBuckets.get(bucket);
            if (topicBlockingQueue == null) {
                topicBlockingQueue = new TopicLinkedBlockingQueue<>();
                consumeMessageBuckets.put(bucket, topicBlockingQueue);
                ReadTopicTask task = new ReadTopicTask(parent, bucket, topicBlockingQueue);
                task.registerConsumer(consumer);
                executor.execute(task);
            }
        }

    }

    /**
     * 关联Topic与其队列
     */
    private Map<String, LimitBytesBlockingQueue<DefaultBytesMessage>> storeMsg2TopicMap = new HashMap<>();

    /**
     * 关联Queue与其队列
     */
    private Map<String, LimitBytesBlockingQueue<DefaultBytesMessage>> storeMsg2QueueMap = new HashMap<>();

    /**
     * 将消息存储到Topic
     *
     * @param topic
     * @param message
     */
    public void putMessageToTopic(String topic, Message message) {

        if (storeMsg2TopicMap.get(topic) == null) {
            synchronized (this) {
                if (storeMsg2TopicMap.get(topic) == null) {
                    LimitBytesBlockingQueue<DefaultBytesMessage> queue = new LimitBytesBlockingQueue<>();
                    storeMsg2TopicMap.put(topic, queue);
                    WriteTopicTask writeTopicTask = new WriteTopicTask(parent, topic, queue);
                    executor.execute(writeTopicTask);
                }
            }
        }

        LimitBytesBlockingQueue<DefaultBytesMessage> queue = storeMsg2TopicMap.get(topic);
        try {
            queue.put((DefaultBytesMessage) message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * 将消息存储到Queue
     *
     * @param queueName
     * @param message
     */
    public void putMessageToQueue(String queueName, Message message) {

        if (storeMsg2QueueMap.get(queueName) == null) {
            synchronized (this) {
                if (storeMsg2QueueMap.get(queueName) == null) {
                    LimitBytesBlockingQueue<DefaultBytesMessage> queue = new LimitBytesBlockingQueue<>();
                    storeMsg2QueueMap.put(queueName, queue);
                    WriteQueueTask writeQueueTask = new WriteQueueTask(parent, queueName, queue);
                    executor.execute(writeQueueTask);
                }
            }
        }

        LimitBytesBlockingQueue<DefaultBytesMessage> queue = storeMsg2QueueMap.get(queueName);
        try {
            queue.put((DefaultBytesMessage) message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }




}
