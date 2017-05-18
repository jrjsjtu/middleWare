package io.openmessaging.demo;

/**
 * Created by Xingfeng on 2017-05-17.
 */
public class NoticiMessagePool {

    /**
     * The maximum number of bytes to pool.
     */
    static final long MAX_SIZE = 4 * 1024 * 1024; //4MB

    /**
     * Singly-linked list of segments.
     */
    static NoticeMessage next;

    /**
     * Total bytes in this pool.
     */
    static long byteCount;

    private NoticiMessagePool() {
    }

    static NoticeMessage take() {
        synchronized (NoticiMessagePool.class) {
            if (next != null) {
                NoticeMessage result = next;
                next = result.next;
                result.next = null;
                byteCount -= result.getByteCount();
                return result;
            }
        }
        return new NoticeMessage(); // Pool is empty. Don't zero-fill while holding a lock.
    }

    static void recycle(NoticeMessage message) {
        synchronized (NoticiMessagePool.class) {
            if (byteCount + message.getByteCount() > MAX_SIZE) return; // Pool is full.
            byteCount += message.getByteCount();
            message.next = next;
            next = message;
        }
    }

}
