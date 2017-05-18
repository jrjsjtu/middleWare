package io.openmessaging.demo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * 字节串的一个链表结构
 * Created by Xingfeng on 2017-05-17.
 */
public class ByteString implements Iterable<Byte> {

    List<Byte> bytes = new ArrayList<>();


    public ByteString() {
    }

    public ByteString(int capacity) {
        this.bytes = new ArrayList<>(8);
    }


    public void add(byte b) {
        bytes.add(b);
    }

    public void add(ByteString byteString) {
        for (byte b : byteString)
            bytes.add(b);
    }

    public void addAll(byte[] array) {
        for (byte b : array)
            bytes.add(b);
    }

    public int size() {
        return bytes.size();
    }

    @Override
    public Iterator<Byte> iterator() {
        return bytes.iterator();
    }

    @Override
    public String toString() {
        return bytes.toString();
    }

    private int pos = 0;

    public byte[] toArray() {
        pos = 0;
        byte[] array = new byte[size()];
        bytes.stream().forEach(aByte -> array[pos++] = aByte);
        return array;
    }

    /**
     * 反转List
     */
    public void reverse() {
        Collections.reverse(bytes);
    }
}
