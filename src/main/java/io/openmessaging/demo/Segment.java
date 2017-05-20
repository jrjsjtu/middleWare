/*
 * Copyright (C) 2014 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.demo;

/**
 * Segment为循环队列，在Segment池中为单链表结构
 */
final class Segment {

    /**
     * 每一个Segment大小为256字节
     */
    static final int SIZE = 256;

    final byte[] data;

    //下一个读的位置
    int pos;

    //下一个写的位置
    int limit;

    /**
     * 下一个Segment
     */
    Segment next;

    /**
     * 在循环链表中的前一个Segment
     */
    Segment prev;

    Segment() {
        this.data = new byte[SIZE];
    }

    Segment(Segment shareFrom) {
        this(shareFrom.data, shareFrom.pos, shareFrom.limit);
    }

    Segment(byte[] data, int pos, int limit) {
        this.data = data;
        this.pos = pos;
        this.limit = limit;
    }

    /**
     * 删除循环链表中的当前Segment，返回它的后继节点；如果链表为空，都返回null
     */
    public Segment pop() {
        Segment result = next != this ? next : null;
        prev.next = next;
        next.prev = prev;
        next = null;
        prev = null;
        return result;
    }

    /**
     * 将一个Segment添加到循环队列的尾部，并返回该Segment
     */
    public Segment push(Segment segment) {
        segment.prev = this;
        segment.next = next;
        next.prev = segment;
        next = segment;
        return segment;
    }

    /**
     * 将循环队列的头Segment分隔成两个Segment，第一个Segment包含的数据为[pos,pos+byteCount)
     * 第二个Segment包含的数据为[pos+byteCount,limit)
     * 返回该链表的新头
     */
    public Segment split(int byteCount) {
        if (byteCount <= 0 || byteCount > limit - pos) throw new IllegalArgumentException();
        Segment prefix = SegmentPool.take();
        System.arraycopy(data, pos, prefix.data, 0, byteCount);
        prefix.limit = prefix.pos + byteCount;
        pos += byteCount;
        prev.push(prefix);
        return prefix;
    }

    /**
     * 当尾部和它的前继节点均小于半满时，将调用该方法进行合并
     */
    public void compact() {
        if (prev == this) throw new IllegalStateException();
        int byteCount = limit - pos;
        int availableByteCount = SIZE - prev.limit + prev.pos;
        if (byteCount > availableByteCount) return;
        writeTo(prev, byteCount);
        pop();
        SegmentPool.recycle(this);
    }

    /**
     * 从本Segment转移byteCount个字节到sink
     */
    public void writeTo(Segment sink, int byteCount) {
        if (sink.limit + byteCount > SIZE) {
            // We can't fit byteCount bytes at the sink's current position. Shift sink first.
            if (sink.limit + byteCount - sink.pos > SIZE) throw new IllegalArgumentException();
            System.arraycopy(sink.data, sink.pos, sink.data, 0, sink.limit - sink.pos);
            sink.limit -= sink.pos;
            sink.pos = 0;
        }

        System.arraycopy(data, pos, sink.data, sink.limit, byteCount);
        sink.limit += byteCount;
        pos += byteCount;
    }
}
