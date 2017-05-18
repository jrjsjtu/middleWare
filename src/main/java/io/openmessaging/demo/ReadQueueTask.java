package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.io.*;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Xingfeng on 2017-05-16.
 */
public class ReadQueueTask implements Runnable {

    private String parent;
    private String queueName;
    private BlockingQueue<Message> queue;
    private DefaultPullConsumer boundConsumer;

    private MessageDecoder messageDecoder = new PBMessageDecoder();

    public ReadQueueTask(String parent, String queueName, BlockingQueue<Message> queue) {
        this.parent = parent;
        this.queueName = queueName;
        this.queue = queue;
    }

    public void registerConsumer(DefaultPullConsumer consumer) {
        boundConsumer = consumer;
    }

    @Override
    public void run() {

        try {
            File file = new File(parent, queueName);
            if (file.exists()) {

                BufferedReader reader = null;
                FileInputStream in = null;
                try {
                    in = new FileInputStream(file);
                    DefaultBytesMessage message = null;
                    while ((message = messageDecoder.bytes2Message(in)) != null) {
                        NoticeMessage noticeMessage = new NoticeMessage(MessageHeader.QUEUE, queueName);
                        noticeMessage.setAttach(message);
//                        queue.put(message);
                        boundConsumer.sendNoticeMessage(noticeMessage);
                    }
//                    reader = new BufferedReader(new InputStreamReader(in));
//
//                    String line = null;
//                    DefaultBytesMessage defaultBytesMessage = null;
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
//                        queue.put(defaultBytesMessage);
//                        boundConsumer.sendNoticeMessage(new NoticeMessage(MessageHeader.QUEUE, queueName));
//                    }

                } finally {

                    IOUtil.close(reader);
                    IOUtil.close(in);

                    //设置阻塞队列已满的状态
//                    ((TopicLinkedBlockingQueue) queue).setStopPut(true);
                    boundConsumer.sendNoticeMessage(NoticeMessage.END_QUEUE_MESSAGE);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
