package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;

import java.nio.ByteBuffer;

public class DefaultMessageFactory implements MessageFactory {

    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        defaultBytesMessage.putHeaders(MessageHeader.TOPIC, topic);
        return defaultBytesMessage;
    }

    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        defaultBytesMessage.putHeaders(MessageHeader.QUEUE, queue);
        return defaultBytesMessage;
    }

    public Message toMessage(byte[] byteArray){
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        byteBuffer.rewind();
        int len = byteBuffer.getInt();
        byte[] body = new byte[len];
        byteBuffer.get(body);
        DefaultBytesMessage message = new DefaultBytesMessage(body);
        int strlen,vallen;byte[] tmpkey,tmpvalue;String key,valuestr;
        while (true){
            char tmp = byteBuffer.getChar();
            if (tmp == ' ') break;
            switch (tmp){
                case '1':
                    int headerInt = byteBuffer.getInt();
                    strlen = byteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    byteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putHeaders(key,headerInt);break;
                case '2':
                    long headerLong = byteBuffer.getLong();
                    strlen = byteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    byteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putHeaders(key,headerLong);break;
                case '3':
                    double headerDouble = byteBuffer.getDouble();
                    strlen = byteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    byteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putHeaders(key,headerDouble);break;
                case '4':
                    strlen = byteBuffer.getInt();
                    tmpvalue = new byte[strlen];
                    byteBuffer.get(tmpvalue);
                    valuestr = new String(tmpvalue);
                    strlen = byteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    byteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putHeaders(key,valuestr);break;
            }
        }

        while (true){
            char tmp = byteBuffer.getChar();
            if (tmp == ' ') break;
            switch (tmp){
                case '1':
                    int headerInt = byteBuffer.getInt();
                    strlen = byteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    byteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putProperties(key,headerInt);break;
                case '2':
                    long headerLong = byteBuffer.getLong();
                    strlen = byteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    byteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putProperties(key,headerLong);break;
                case '3':
                    double headerDouble = byteBuffer.getDouble();
                    strlen = byteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    byteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putProperties(key,headerDouble);break;
                case '4':
                    strlen = byteBuffer.getInt();
                    tmpvalue = new byte[strlen];
                    byteBuffer.get(tmpvalue);
                    valuestr = new String(tmpvalue);
                    strlen = byteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    byteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putProperties(key,valuestr);break;
            }
        }
        return message;
    }
}
