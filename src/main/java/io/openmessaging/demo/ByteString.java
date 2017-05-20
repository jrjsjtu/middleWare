package io.openmessaging.demo;

import java.util.*;
import java.util.function.Consumer;

/**
 * 字节串的一个链表结构
 * Created by Xingfeng on 2017-05-17.
 */
public class ByteString implements Iterable<Byte> {

    byte[] elementData;
    int size = 0;

    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    public ByteString() {
        elementData = new byte[10];
    }

    public ByteString(int capacity) {
        elementData = new byte[capacity];
    }

    private void ensureCapacityInternal(int minCapacity) {
        ensureExplicitCapacity(minCapacity);
    }

    private void ensureExplicitCapacity(int minCapacity) {
        if (minCapacity - elementData.length > 0)
            grow(minCapacity);
    }

    private void grow(int minCapacity) {
        int oldCapacity = elementData.length;
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;
        if (newCapacity - MAX_ARRAY_SIZE > 0)
            newCapacity = hugeCapacity(minCapacity);
        // minCapacity is usually close to size, so this is a win:
        elementData = Arrays.copyOf(elementData, newCapacity);
    }

    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) // overflow
            throw new OutOfMemoryError();
        return (minCapacity > MAX_ARRAY_SIZE) ?
                Integer.MAX_VALUE :
                MAX_ARRAY_SIZE;
    }

    public void add(byte b) {
        ensureCapacityInternal(size + 1);  // Increments modCount!!
        elementData[size++] = b;
    }

    public void add(ByteString byteString) {
        for (byte b : byteString)
            add(b);
    }

    public void addAll(byte[] array) {
        for (byte b : array)
            add(b);
    }

    public int size() {
        return size;
    }

    @Override
    public Iterator<Byte> iterator() {
        return new Itr();
    }

    /**
     * An optimized version of AbstractList.Itr
     */
    private class Itr implements Iterator<Byte> {
        int cursor;
        int lastRet = -1;

        public boolean hasNext() {
            return cursor != size;
        }

        public Byte next() {
            int i = cursor;
            if (i >= size)
                throw new NoSuchElementException();
            byte[] elementData = ByteString.this.elementData;
            if (i >= elementData.length)
                throw new ConcurrentModificationException();
            cursor = i + 1;
            return elementData[lastRet = i];
        }

        public void remove() {
        }

    }

    public byte[] toArray() {
        byte[] data = new byte[size];
        System.arraycopy(elementData, 0, data, 0, data.length);
        return data;
    }

    /**
     * 反转List
     */
    public void reverse() {

        for (int i = 0, j = size - 1; i < j; i++, j--) {
            swap(i, j);
        }

    }

    private void swap(int i, int j) {
        byte temp = elementData[i];
        elementData[i] = elementData[j];
        elementData[j] = temp;
    }
}
