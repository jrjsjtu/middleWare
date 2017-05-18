package io.openmessaging.demo;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Xingfeng on 2017-05-15.
 */
public class EasyMessageEncoder implements MessageEncoder {

    //换行符
    private static final String SEPARATOR = System.getProperty("line.separator");
    //换行符的字节数组
    private static final byte[] SEPARATOR_BYTES = SEPARATOR.getBytes();

    @Override
    public byte[] message2Bytes(DefaultBytesMessage message) {

        List<Byte> bytesList = new ArrayList<>();

        //写入headers行
        byte[] data = (message.getHeadersLine() + SEPARATOR).getBytes();
        for (int i = 0; i < data.length; i++) {
            bytesList.add(data[i]);
        }

        //写入properties行
        data = (message.getPropertiesLine() + SEPARATOR).getBytes();
        for (int i = 0; i < data.length; i++) {
            bytesList.add(data[i]);
        }


        byte[] body = message.getBody();
        //首先写入body长度
        data = (String.valueOf(body.length) + SEPARATOR).getBytes();
        for (int i = 0; i < data.length; i++) {
            bytesList.add(data[i]);
        }

        //写入body部分
        for (int i = 0; i < body.length; i++) {
            bytesList.add(body[i]);
        }
        //添加换行符
        for (int i = 0; i < SEPARATOR_BYTES.length; i++) {
            bytesList.add(SEPARATOR_BYTES[i]);
        }

        byte[] result = new byte[bytesList.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = bytesList.get(i);
        }

        return result;
    }
}
