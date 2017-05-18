package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.PullConsumer;
import io.openmessaging.tester.ConsumerTester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DefaultPullConsumer implements PullConsumer {

    static Logger logger = LoggerFactory.getLogger(ConsumerTester.class);

    private MessageStore messageStore = null;
    private KeyValue properties;
    private String queue;
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();

    private Map<String, Integer> topicOffsetMap = new HashMap<>();

    //通知队列
    private BlockingQueue<NoticeMessage> noticeQueue = new LinkedBlockingQueue<>(1024 * 1024);

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        messageStore = MessageStore.getInstance(properties.getString("STORE_PATH"));
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message poll() {
        if (buckets.size() == 0 && queue == null) {
            return null;
        }

        try {
            NoticeMessage notice;
            while ((notice = noticeQueue.take()) != null) {

                if (notice == NoticeMessage.END_QUEUE_MESSAGE) {
                    queue = null;
                    if (bucketList.size() == 0)
                        break;
                    continue;
                }

                if (notice.getKey().equals(NoticeMessage.END_TOPIC)) {
                    String topicName = notice.getValue();
                    bucketList.remove(topicName);
//                    NoticiMessagePool.recycle(notice);
                    if (bucketList.size() == 0 && queue == null)
                        break;
                    continue;
                }


                if (notice.getKey().equals(MessageHeader.QUEUE)) {
                    DefaultBytesMessage message = notice.getAttach();
//                    NoticiMessagePool.recycle(notice);
                    if (message != null)
                        return message;
//                    Message message = messageStore.pullMessage(queue);
//                    if (message != null)
//                        return message;
                } else {

                    String value = notice.getValue();
                    for (String bucket : bucketList) {

                        if (bucket.equals(value)) {

                            int offset = topicOffsetMap.getOrDefault(bucket, 0);
                            Message message = messageStore.pullMessage(bucket, offset);
                            if (message != null) {
                                topicOffsetMap.put(bucket, offset + 1);
                                return message;
                            }
                        }
                    }
                }


            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return null;
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
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        buckets.addAll(topics);
        bucketList.clear();
        bucketList.addAll(buckets);

        messageStore.registerConsumers(this, queueName, topics);

    }

    public void sendNoticeMessage(NoticeMessage notice) {
        try {
            noticeQueue.put(notice);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
