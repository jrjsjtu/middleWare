package io.openmessaging.demo;

/**
 * 通知消息，读队列一条消息，读队列任务结束消息，读topic一条消息
 * Created by Xingfeng on 2017-05-16.
 */
public class NoticeMessage {

    public static final String END_QUEUE = "END_QUEUE";
    public static final String END_TOPIC = "END_TOPIC";

    public static final NoticeMessage END_QUEUE_MESSAGE = new NoticeMessage(END_QUEUE, END_QUEUE);
    private String key;
    private String value;
    private DefaultBytesMessage attach;

    NoticeMessage next;

    public NoticeMessage() {
    }

    public NoticeMessage(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public DefaultBytesMessage getAttach() {
        return attach;
    }

    public void setAttach(DefaultBytesMessage attach) {
        this.attach = attach;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public int getByteCount() {
        if (attach != null)
            return attach.getByteCount();
        return 0;
    }

    @Override
    public String toString() {
        return "NoticeMessage{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
