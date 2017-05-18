package io.openmessaging.demo;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;

/**
 * Created by Xingfeng on 2017-05-16.
 */
public class PBTest {

    public static void main(String[] args) throws UnsupportedEncodingException {

        DefaultBytesMessage message = new DefaultBytesMessage("123".getBytes(PBUtil.UTF_8));
        DefaultKeyValue headerKV = new DefaultKeyValue();
        headerKV.put("TOPIC_0", 1);
        headerKV.put("TOPIC_1", 1);
        headerKV.put("TOPIC_2", 1.0);
        headerKV.put("TOPIC_3", "123");
        message.setHeaders(headerKV);

        DefaultKeyValue propertiesKV = new DefaultKeyValue();
        propertiesKV.put("TOPIC_0", 1);
        propertiesKV.put("TOPIC_1", 1L);
        propertiesKV.put("TOPIC_2", 1.0);
        propertiesKV.put("TOPIC_3", "123");
        message.setProperties(propertiesKV);

        byte[] body = new PBMessageEncoder().message2Bytes(message);

        ByteArrayInputStream in = new ByteArrayInputStream(body);

        DefaultBytesMessage defaultBytesMessage = new PBMessageDecoder().bytes2Message(in);
        System.out.println(defaultBytesMessage);

        byte[] data = TypeConverter.doubleToBytes(1.23);
        System.out.println(TypeConverter.bytesToDouble(data));

    }

}
