package io.openmessaging.demo;

import java.util.LinkedList;

/**
 * Created by Xingfeng on 2017-05-19.
 */
public class MessagePool {

    static final long MAX_SIZE = 20 * 1024 * 1024; //20MB

    static LinkedList<DefaultBytesMessage> messageLinkedList = new LinkedList<>();

    /**
     * Total bytes in this pool.
     */
    static long byteCount;

    private MessagePool() {
    }

    static DefaultBytesMessage take() {
        synchronized (MessagePool.class) {
            if (messageLinkedList.size() > 0) {
                DefaultBytesMessage result = messageLinkedList.removeFirst();
                byteCount -= result.getByteCount();
                return result;
            }
        }
        return new DefaultBytesMessage(null); // Pool is empty. Don't zero-fill while holding a lock.
    }

    static void recycle(DefaultBytesMessage message) {
        if (message == null) throw new IllegalArgumentException();
        synchronized (MessagePool.class) {
            if (byteCount + message.getByteCount() > MAX_SIZE) return; // Pool is full.
            byteCount += message.getByteCount();
            message.clear();
            messageLinkedList.offer(message);
        }
    }

}
