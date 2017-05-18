package io.openmessaging.demo;

import java.io.UnsupportedEncodingException;

/**
 * Created by Xingfeng on 2017-05-16.
 */
public class PBUtil {

    public static final String UTF_8 = "utf-8";

    /**
     * 将int转成ByteString
     *
     * @param value
     * @return
     */
    public static ByteString int2Bytes(int value) {

        ByteString byteString = new ByteString();

        if (value == 0) {
            byteString.add((byte) 0);
            return byteString;
        }

        boolean isLast = true;
        while (value != 0) {

            byte temp = (byte) (value & 0X7F);
            if (isLast) {
                byteString.add(temp);
                isLast = false;
            } else {
                temp = (byte) (temp | 0X80);
                byteString.add(temp);
            }
            value = value >>> 7;

        }

        byteString.reverse();

        return byteString;
    }

    /**
     * 将int转成ByteString
     *
     * @param value
     * @return
     */
    public static ByteString long2Bytes(long value) {

        ByteString byteString = new ByteString();

        if (value == 0) {
            byteString.add((byte) 0);
            return byteString;
        }

        boolean isLast = true;
        while (value != 0) {

            byte temp = (byte) (value & 0X7F);
            if (isLast) {
                byteString.add(temp);
                isLast = false;
            } else {
                temp = (byte) (temp | 0X80);
                byteString.add(temp);
            }
            value = value >>> 7;

        }

        byteString.reverse();

        return byteString;
    }

    /**
     * 将double转成ByteString
     *
     * @param value
     * @return
     */
    public static ByteString double2Bytes(double value) {

        byte[] buffer = TypeConverter.doubleToBytes(value);
        ByteString byteString = new ByteString(8);
        byteString.addAll(buffer);
        return byteString;

    }

    /**
     * 将string转成ByteString
     *
     * @param value
     * @return
     */
    public static ByteString string2Bytes(String value) {

        ByteString byteString = new ByteString();
        //添加尺寸
        int size = value.length();
        ByteString sizeBytes = int2Bytes(size);
        byteString.add(sizeBytes);

        //添加String主体部分
        try {
            byte[] body = value.getBytes(UTF_8);
            byteString.addAll(body);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return byteString;

    }

    public static ByteString byteArray2Bytes(byte[] body) {

        ByteString byteString = new ByteString();
        //添加尺寸
        int size = body.length;
        ByteString sizeBytes = int2Bytes(size);
        byteString.add(sizeBytes);
        //添加主体部分
        byteString.addAll(body);
        return byteString;

    }

    /**
     * 将字节数组转成int
     *
     * @param byteString
     * @return
     */
    public static int bytes2Int(ByteString byteString) {

        int value = 0;
        //反转
        byteString.reverse();
        int shift = 7;
        for (byte b : byteString) {

            if (b >= 0) {
                value += b;
            } else {
                //取后7位
                value += ((b & 0X7F) << shift);
                shift += 7;
            }

        }

        return value;

    }

    public static long bytes2Long(ByteString byteString) {

        long value = 0L;
        //反转
        byteString.reverse();
        int shift = 7;
        for (byte b : byteString) {

            if (b >= 0) {
                value += b;
            } else {
                //取后7位
                value += ((b & 0X7F) << shift);
                shift += 7;
            }

        }

        return value;

    }

    public static double bytes2Double(byte[] buffer) {
        return TypeConverter.bytesToDouble(buffer);
    }
}
