package io.openmessaging.demo;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * * 采取类protocol buffer序列化方式
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
 * Created by Xingfeng on 2017-05-17.
 */
public class PBMessageDecoder implements MessageDecoder {

    @Override
    public DefaultBytesMessage bytes2Message(InputStream in) {

        try {

            //记录DefaultMessage读的字节数
            int byteCount = 0;

            int c = in.read();
            if (c == -1) {
                return null;
            }

            byteCount++;

            DefaultKeyValue headers = new DefaultKeyValue();
            //解析Headers
            byte firstTagAndType = (byte) c;
            byte b = -1;
            byte tag = (byte) (firstTagAndType >>> 3);
            byte type = (byte) (firstTagAndType & 0X07);
            String key = null;
            if (tag == 1 && type == 5) {
                int size = readInt(in, byteCount);
                //读KV消息
                for (int i = 0; i < size; i++) {
                    readKVMessage(in, headers, byteCount);
                }

            }

            //解析Properties或body部分
            DefaultKeyValue properties = new DefaultKeyValue();
            byte secondTagAndSize = (byte) in.read();
            byteCount++;
            tag = (byte) (secondTagAndSize >>> 3);
            type = (byte) (secondTagAndSize & 0X07);
            //如果Properties存在
            if (tag == 2) {
                int size = readInt(in, byteCount);
                //读KV消息
                for (int i = 0; i < size; i++) {
                    readKVMessage(in, properties, byteCount);
                }

                byte[] messageBody = readByteBody(in, byteCount);
                DefaultBytesMessage bytesMessage = new DefaultBytesMessage(messageBody);
                bytesMessage.setHeaders(headers);
                bytesMessage.setProperties(properties);
                bytesMessage.setByteCount(byteCount);
                return bytesMessage;

            }
            //如果不存在
            else {

                byte[] messageBody = readByteBody(in, byteCount);
                DefaultBytesMessage bytesMessage = new DefaultBytesMessage(messageBody);
                bytesMessage.setHeaders(headers);
                bytesMessage.setProperties(properties);
                bytesMessage.setByteCount(byteCount);
                return bytesMessage;

            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 从输入流中读取一个KV消息
     *
     * @param in
     * @param headers
     * @throws IOException
     */
    private void readKVMessage(InputStream in, DefaultKeyValue headers, int byteCount) throws IOException {

        //读取键的类型和tag
        byte b = (byte) in.read();
        byte tag = (byte) (b >>> 3);
        byte type = (byte) (b & 0X07);

        byteCount++;

        String key = "";
        if (tag == 1 && type == 3) {
            key = readString(in, byteCount);
        }
        //读取值的类型和tag
        b = (byte) in.read();
        tag = (byte) (b >>> 3);
        type = (byte) (b & 0X07);
        if (tag == 2) {

            switch (type) {
                case 0: {
                    int val = readInt(in, byteCount);
                    headers.put(key, val);
                    break;
                }

                case 1: {
                    long val = readLong(in);
                    headers.put(key, val);
                    break;
                }
                case 2: {
                    double val = readDouble(in);
                    headers.put(key, val);
                    break;
                }
                case 3: {
                    String val = readString(in, byteCount);
                    headers.put(key, val);
                    break;
                }
            }

        }

    }

    /**
     * 从输入流中读出一个int值
     *
     * @param in
     * @return
     */
    private int readInt(InputStream in, int byteCount) throws IOException {

        ByteString byteString = new ByteString();
        byte c = -1;
        while (true) {

            c = (byte) in.read();
            byteCount++;
            if (c >= 0) {
                byteString.add(c);
                break;
            } else {
                byteString.add(c);
            }
        }

        int size = PBUtil.bytes2Int(byteString);

        return size;

    }

    /**
     * 从输入流中读出一个long值
     *
     * @param in
     * @return
     * @throws IOException
     */
    private long readLong(InputStream in) throws IOException {

        ByteString byteString = new ByteString();
        byte c = -1;
        while (true) {

            c = (byte) in.read();
            if (c >= 0) {
                byteString.add(c);
                break;
            } else {
                byteString.add(c);
            }
        }

        long size = PBUtil.bytes2Long(byteString);

        return size;

    }

    /**
     * 从输入流中读出一个double值
     *
     * @param in
     * @return
     */
    private double readDouble(InputStream in) throws IOException {

        byte[] buffer = new byte[8];
        in.read(buffer);
        return PBUtil.bytes2Double(buffer);
    }

    private String readString(InputStream in, int byteCount) throws IOException {
        int size = readInt(in, byteCount);
        byte[] buffer = new byte[size];
        in.read(buffer);
        byteCount += size;
        return new String(buffer, 0, buffer.length, PBUtil.UTF_8);
    }

    private byte[] readByteBody(InputStream in, int byteCount) throws IOException {
        int size = readInt(in, byteCount);
        byte[] buffer = new byte[size];
        in.read(buffer);
        byteCount += size;
        return buffer;
    }


}
