package io.openmessaging.demo;

import java.util.LinkedList;
import java.util.List;

/**
 * 采取类protocol buffer序列化方式
 * 主要包含两个message，分别为KV键值对以及Message消息
 * message KV{
 * required string key=1;
 * required object value=2;
 * }
 * <p>
 * message Message{
 * required KV headers=1;
 * repeated KV properties=2;
 * required bytes body=3;
 * }
 * <p>
 * 类型分别如下：
 * int   0
 * long  1
 * double 2
 * string 3
 * bytes 4
 * message 5
 * 其中int、long采用varint方式，double采用64位，bytes、string和message采用length-Dilimited
 * <p>
 * 对于KV的值，根据类型来确定后面解析的策略
 * Created by Xingfeng on 2017-05-16.
 */
public class PBMessageEncoder implements MessageEncoder {

    /**
     * KV中的值类型为int的byte值
     */
    public static final byte KV_INT_TYPE = 16;

    /**
     * KV中的值类型为long的byte值
     */
    public static final byte KV_LONG_TYPE = 17;

    /**
     * KV中的值类型为double的byte值
     */
    public static final byte KV_DOUBLE_TYPE = 18;

    /**
     * KV中的值类型为string的byte值
     */
    public static final byte KV_STRING_TYPE = 19;

    @Override
    public Buffer message2Bytes(DefaultBytesMessage message) {

        Buffer buffer = new Buffer();

        DefaultKeyValue headers = (DefaultKeyValue) message.headers();
        DefaultKeyValue properties = (DefaultKeyValue) message.properties();
        byte[] body = message.getBody();
        //消息Header部分
        pbEncodeKeyValue(buffer, headers, 1);

        //消息Properties部分
        if (properties != null) {
            pbEncodeKeyValue(buffer, properties, 2);
        }

        //消息主体部分
        pbEncodeMsgBody(buffer, body);

        return buffer;
    }

    /**
     * 编码Message的body字节数组部分
     *
     * @return
     */
    private void pbEncodeMsgBody(Buffer buffer, byte[] body) {
        //添加body TAG
        buffer.writeByte((byte) 28);
        //添加body
        buffer.write(body);
    }


    /**
     * 编码Header和Properties
     *
     * @param buffer
     * @param keyValue
     * @param tag
     * @return
     */
    private void pbEncodeKeyValue(Buffer buffer, DefaultKeyValue keyValue, int tag) {

        //添加类型和TAG
        byte typeAndTag = (byte) ((tag << 3) + 5);
        buffer.writeByte(typeAndTag);
        //添加KV消息个数
        int kvMsgSize = keyValue.keySet().size();
        for (byte b : PBUtil.int2Bytes(kvMsgSize))
            buffer.writeByte(b);

        //一条K-V对的序列化后的字节
        for (String key : keyValue.keySet()) {
            pbEncodeKVMessage(buffer, key, keyValue.getNewValue(key));
        }
    }

    /**
     * 将KV消息编码成ByteString
     *
     * @param buffer
     * @param key
     * @param newValue
     * @return
     */
    private void pbEncodeKVMessage(Buffer buffer, String key, DefaultKeyValue.NewValue newValue) {

        ByteString byteString = new ByteString();

        //编码Key=1
        buffer.writeByte((byte) 11);
        //编码key
        PBUtil.string2Bytes(buffer, key);

        //编码newValue
        int type = newValue.type;
        switch (type) {

            case 0:

                buffer.writeByte(KV_INT_TYPE);
                for (byte b : PBUtil.int2Bytes(newValue.intValue))
                    buffer.writeByte(b);

                break;

            case 1:

                buffer.writeByte(KV_LONG_TYPE);
                for (byte b : PBUtil.long2Bytes(newValue.longValue))
                    buffer.writeByte(b);

                break;

            case 2:

                buffer.writeByte(KV_DOUBLE_TYPE);
                for (byte b : PBUtil.double2Bytes(newValue.doubleValue))
                    buffer.writeByte(b);

                break;

            case 3:

                buffer.writeByte(KV_STRING_TYPE);
                PBUtil.string2Bytes(buffer, newValue.stringValue);

                break;

        }

    }


}
