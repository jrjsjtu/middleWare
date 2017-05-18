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
    public byte[] message2Bytes(DefaultBytesMessage message) {

        DefaultKeyValue headers = (DefaultKeyValue) message.headers();
        DefaultKeyValue properties = (DefaultKeyValue) message.properties();
        byte[] body = message.getBody();

        int totalSize = 0;
        List<ByteString> list = new LinkedList<>();
        //消息Header部分
        ByteString byteString = pbEncodeKeyValue(headers, 1);
        totalSize += byteString.size();
        list.add(byteString);

        //消息Properties部分
        if (properties != null) {
            byteString = pbEncodeKeyValue(properties, 2);
            totalSize += byteString.size();
            list.add(pbEncodeKeyValue(properties, 2));
        }

        //消息主体部分
        byteString = pbEncodeMsgBody(body);
        totalSize += byteString.size();
        list.add(pbEncodeMsgBody(body));

        byte[] encodeBytes = new byte[totalSize];
        int pos = 0;
        for (ByteString bs : list) {
            for (byte b : bs) {
                encodeBytes[pos++] = b;
            }
        }

        return encodeBytes;
    }

    /**
     * 编码Message的body字节数组部分
     *
     * @return
     */
    private ByteString pbEncodeMsgBody(byte[] body) {
        ByteString byteString = new ByteString();
        //添加body TAG
        byteString.add((byte) 28);
        //添加body
        byteString.add(PBUtil.byteArray2Bytes(body));
        return byteString;
    }


    /**
     * 编码Header和Properties
     *
     * @param keyValue
     * @param tag
     * @return
     */
    private ByteString pbEncodeKeyValue(DefaultKeyValue keyValue, int tag) {

        ByteString byteString = new ByteString();
        //添加类型和TAG
        byte typeAndTag = (byte) ((tag << 3) + 5);
        byteString.add(typeAndTag);
        //添加KV消息个数
        int kvMsgSize = keyValue.keySet().size();
        for (byte b : PBUtil.int2Bytes(kvMsgSize))
            byteString.add(b);

        //一条K-V对的序列化后的字节
        for (String key : keyValue.keySet()) {
            for (byte b : pbEncodeKVMessage(key, keyValue.getNewValue(key)))
                byteString.add(b);
        }

        return byteString;
    }

    /**
     * 将KV消息编码成ByteString
     *
     * @param key
     * @param newValue
     * @return
     */
    private ByteString pbEncodeKVMessage(String key, DefaultKeyValue.NewValue newValue) {

        ByteString byteString = new ByteString();

        //编码Key=1
        byteString.add((byte) 11);
        //编码key
        for (byte b : PBUtil.string2Bytes(key))
            byteString.add(b);

        //编码newValue
        int type = newValue.type;
        switch (type) {

            case 0:

                byteString.add(KV_INT_TYPE);
                for (byte b : PBUtil.int2Bytes(newValue.intValue))
                    byteString.add(b);

                break;

            case 1:

                byteString.add(KV_LONG_TYPE);
                for (byte b : PBUtil.long2Bytes(newValue.longValue))
                    byteString.add(b);

                break;

            case 2:

                byteString.add(KV_DOUBLE_TYPE);
                for (byte b : PBUtil.double2Bytes(newValue.doubleValue))
                    byteString.add(b);

                break;

            case 3:


                byteString.add(KV_STRING_TYPE);
                for (byte b : PBUtil.string2Bytes(newValue.stringValue))
                    byteString.add(b);

                break;

        }

        return byteString;
    }


}
