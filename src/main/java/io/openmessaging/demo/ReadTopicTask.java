package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * 将消息从磁盘读取到内存中的任务
 * Created by Xingfeng on 2017-05-15.
 */
public class ReadTopicTask implements Runnable {

    private String parent;
    private String fileName;
    private BlockingQueue<Message> queue;
    private List<DefaultPullConsumer> consumers = new ArrayList<>();

    private MessageDecoder messageDecoder = new PBMessageDecoder();

    //4 MB
    private static final int MAX_SIZE = 4 * 1024 * 1024;

    private LruCache<Integer, DefaultBytesMessage> memoryMessages = new LruCache<Integer, DefaultBytesMessage>(MAX_SIZE) {
        @Override
        protected int sizeOf(Integer key, DefaultBytesMessage value) {
            return value.getByteCount();
        }
    };

    /**
     * 从内存中获取消息
     *
     * @param offset
     * @return
     */
    public DefaultBytesMessage getMessageFromMemory(int offset) {
        return memoryMessages.get(offset);
    }

    /**
     * 将消息存入内存中
     *
     * @param offset
     * @param message
     */
    public void putMessageToMemory(int offset, DefaultBytesMessage message) {

        if (memoryMessages.get(offset) == null) {
            memoryMessages.put(offset, message);
        }

    }

    public ReadTopicTask(String parent, String fileName, BlockingQueue<Message> queue) {
        this.parent = parent;
        this.fileName = fileName;
        this.queue = queue;
    }

    public void registerConsumer(DefaultPullConsumer consumer) {
        consumers.add(consumer);
    }

    public void unregisterConsumer(DefaultPullConsumer consumer) {
        consumers.remove(consumer);
    }

    @Override
    public void run() {

        try {
            File file = new File(parent, fileName);
            if (file.exists()) {

                BufferedReader reader = null;
                FileInputStream in = null;
                try {
                    in = new FileInputStream(file);
                    reader = new BufferedReader(new InputStreamReader(in));

                    String line = null;
                    int len = 0;
                    DefaultBytesMessage defaultBytesMessage = null;
                    while ((defaultBytesMessage = messageDecoder.bytes2Message(in)) != null) {
                        queue.offer(defaultBytesMessage);
                        NoticeMessage noticeMessage = new NoticeMessage(MessageHeader.TOPIC, fileName);
                        for (DefaultPullConsumer consumer : consumers) {
                            consumer.sendNoticeMessage(noticeMessage);
                        }
                    }
//                    while ((line = reader.readLine()) != null) {
//
//                        //headers的kv对
//                        DefaultKeyValue headers = DefaultKeyValue.parseKVLine(line);
//
//                        //properties的kv对
//                        line = reader.readLine().trim();
//                        DefaultKeyValue properties = DefaultKeyValue.parseKVLine(line);
//
//                        //body部分的长度
//                        int bodyLength = Integer.parseInt(reader.readLine());
//                        line = reader.readLine();
//                        byte[] body = line.getBytes();
//
//                        //组装消息
//                        defaultBytesMessage = new DefaultBytesMessage(body);
//                        defaultBytesMessage.setHeaders(headers);
//                        defaultBytesMessage.setProperties(properties);
//                        queue.offer(defaultBytesMessage);
//                        NoticeMessage noticeMessage = new NoticeMessage(MessageHeader.TOPIC, fileName);
//                        for (DefaultPullConsumer consumer : consumers) {
//                            consumer.sendNoticeMessage(noticeMessage);
//                        }
//                    }

                } finally {

                    if (reader != null)
                        reader.close();

                    if (in != null) {
                        in.close();
                    }

                    //设置阻塞队列已满的状态
                    NoticeMessage endTopicMessage = new NoticeMessage(NoticeMessage.END_TOPIC, fileName);
                    for (DefaultPullConsumer consumer : consumers)
                        consumer.sendNoticeMessage(endTopicMessage);

                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
