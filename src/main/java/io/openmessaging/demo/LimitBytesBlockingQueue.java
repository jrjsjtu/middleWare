package io.openmessaging.demo;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 限制字节总数的阻塞队列
 * Created by Xingfeng on 2017-05-18.
 */
public class LimitBytesBlockingQueue<E extends DefaultBytesMessage> extends AbstractQueue<E>
        implements BlockingQueue<E> {

    //20MB
    public static final int MAX_BYTES_COUNT = 10 * 1024 * 1024;

    /**
     * Linked list node class
     */
    static class Node<E> {
        E item;

        /**
         * One of:
         * - the real successor Node
         * - this Node, meaning the successor is head.next
         * - null, meaning there is no successor (this is the last node)
         */
        LimitBytesBlockingQueue.Node<E> next;

        Node(E x) {
            item = x;
        }
    }

    /**
     * 最大字节数
     */
    private final int maxBytesCount;

    /**
     * 当前字节数
     */
    private final AtomicInteger count = new AtomicInteger();

    /**
     * Head of linked list.
     * Invariant: head.item == null
     */
    transient LimitBytesBlockingQueue.Node<E> head;

    /**
     * Tail of linked list.
     * Invariant: last.next == null
     */
    private transient LimitBytesBlockingQueue.Node<E> last;

    /**
     * Lock held by take, poll, etc
     */
    private final ReentrantLock takeLock = new ReentrantLock();

    /**
     * Wait queue for waiting takes
     */
    private final Condition notEmpty = takeLock.newCondition();

    /**
     * Lock held by put, offer, etc
     */
    private final ReentrantLock putLock = new ReentrantLock();

    /**
     * Wait queue for waiting puts
     */
    private final Condition notFull = putLock.newCondition();

    /**
     * Signals a waiting take. Called only from put/offer (which do not
     * otherwise ordinarily lock takeLock.)
     */
    private void signalNotEmpty() {
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }

    /**
     * Signals a waiting put. Called only from take/poll.
     */
    private void signalNotFull() {
        final ReentrantLock putLock = this.putLock;
        putLock.lock();
        try {
            notFull.signal();
        } finally {
            putLock.unlock();
        }
    }

    /**
     * Links node at end of queue.
     *
     * @param node the node
     */
    private void enqueue(LimitBytesBlockingQueue.Node<E> node) {
        // assert putLock.isHeldByCurrentThread();
        // assert last.next == null;
        last = last.next = node;
    }

    /**
     * Removes a node from head of queue.
     *
     * @return the node
     */
    private E dequeue() {
        // assert takeLock.isHeldByCurrentThread();
        // assert head.item == null;
        LimitBytesBlockingQueue.Node<E> h = head;
        LimitBytesBlockingQueue.Node<E> first = h.next;
        h.next = h; // help GC
        head = first;
        E x = first.item;
        first.item = null;
        return x;
    }

    /**
     * Locks to prevent both puts and takes.
     */
    void fullyLock() {
        putLock.lock();
        takeLock.lock();
    }

    /**
     * Unlocks to allow both puts and takes.
     */
    void fullyUnlock() {
        takeLock.unlock();
        putLock.unlock();
    }

    /**
     * Creates a {@code LinkedBlockingQueue} with a maxBytesCount of
     * {@link Integer#MAX_VALUE}.
     */
    public LimitBytesBlockingQueue() {
        this(MAX_BYTES_COUNT);
    }

    /**
     * Creates a {@code LinkedBlockingQueue} with the given (fixed) maxBytesCount.
     *
     * @param maxBytesCount the maxBytesCount of this queue
     * @throws IllegalArgumentException if {@code maxBytesCount} is not greater
     *                                  than zero
     */
    public LimitBytesBlockingQueue(int maxBytesCount) {
        if (maxBytesCount <= 0) throw new IllegalArgumentException();
        this.maxBytesCount = maxBytesCount;
        last = head = new LimitBytesBlockingQueue.Node<E>(null);
    }

    /**
     * Returns the number of elements in this queue.
     *
     * @return the number of elements in this queue
     */
    public int size() {
        return count.get();
    }


    /**
     * Returns the number of additional elements that this queue can ideally
     * (in the absence of memory or resource constraints) accept without
     * blocking. This is always equal to the initial maxBytesCount of this queue
     * less the current {@code size} of this queue.
     * <p>
     * <p>Note that you <em>cannot</em> always tell if an attempt to insert
     * an element will succeed by inspecting {@code remainingCapacity}
     * because it may be the case that another thread is about to
     * insert or remove an element.
     */
    public int remainingCapacity() {
        return maxBytesCount - count.get();
    }

    /**
     * 插入指定消息，当插入会超过最大容量时，将阻塞
     *
     * @throws InterruptedException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     */
    public void put(E e) throws InterruptedException {
        if (e == null) throw new NullPointerException();
        // Note: convention in all put/take/etc is to preset local var
        // holding count negative to indicate failure unless set.
        int c = -1;
        int bytes = e.getByteCount();
        LimitBytesBlockingQueue.Node<E> node = new LimitBytesBlockingQueue.Node<E>(e);
        final ReentrantLock putLock = this.putLock;
        final AtomicInteger count = this.count;
        putLock.lockInterruptibly();
        try {
            //当插入新元素将超过最大字节数时，阻塞
            while (count.get() + bytes > maxBytesCount) {
                notFull.await();
            }
            enqueue(node);
            c = count.getAndAdd(bytes);
            if (c + bytes < maxBytesCount)
                notFull.signal();
        } finally {
            putLock.unlock();
        }
        if (c == 0)
            signalNotEmpty();
    }

    /**
     * Inserts the specified element at the tail of this queue, waiting if
     * necessary up to the specified wait time for space to become available.
     *
     * @return {@code true} if successful, or {@code false} if
     * the specified waiting time elapses before space is available
     * @throws InterruptedException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     */
    public boolean offer(E e, long timeout, TimeUnit unit)
            throws InterruptedException {

        if (e == null) throw new NullPointerException();
        long nanos = unit.toNanos(timeout);
        int c = -1;
        int bytes = e.getByteCount();
        final ReentrantLock putLock = this.putLock;
        final AtomicInteger count = this.count;
        putLock.lockInterruptibly();
        try {
            while (count.get() + bytes > maxBytesCount) {
                if (nanos <= 0)
                    return false;
                nanos = notFull.awaitNanos(nanos);
            }
            enqueue(new LimitBytesBlockingQueue.Node<E>(e));
            c = count.getAndAdd(bytes);
            if (c < maxBytesCount)
                notFull.signal();
        } finally {
            putLock.unlock();
        }
        if (c == 0)
            signalNotEmpty();
        return true;
    }

    /**
     * Inserts the specified element at the tail of this queue if it is
     * possible to do so immediately without exceeding the queue's maxBytesCount,
     * returning {@code true} upon success and {@code false} if this queue
     * is full.
     * When using a maxBytesCount-restricted queue, this method is generally
     * preferable to method {@link BlockingQueue#add add}, which can fail to
     * insert an element only by throwing an exception.
     *
     * @throws NullPointerException if the specified element is null
     */
    public boolean offer(E e) {
        if (e == null) throw new NullPointerException();
        final AtomicInteger count = this.count;
        if (count.get() == maxBytesCount)
            return false;
        int c = -1;
        int bytes = e.getByteCount();
        LimitBytesBlockingQueue.Node<E> node = new LimitBytesBlockingQueue.Node<E>(e);
        final ReentrantLock putLock = this.putLock;
        putLock.lock();
        try {
            if (count.get() + bytes <= maxBytesCount) {
                enqueue(node);
                c = count.getAndAdd(bytes);
                if (c < maxBytesCount)
                    notFull.signal();
            }
        } finally {
            putLock.unlock();
        }
        if (c >= 0)
            signalNotEmpty();
        return c >= 0;
    }

    public E take() throws InterruptedException {
        E x;
        int c = -1;
        final AtomicInteger count = this.count;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        try {
            while (count.get() == 0) {
                notEmpty.await();
            }
            x = dequeue();
            c = count.getAndAdd(-x.getByteCount());
            if (c > x.getByteCount())
                notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
        if (c == maxBytesCount)
            signalNotFull();
        return x;
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E x = null;
        int c = -1;
        long nanos = unit.toNanos(timeout);
        final AtomicInteger count = this.count;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        try {
            while (count.get() == 0) {
                if (nanos <= 0)
                    return null;
                nanos = notEmpty.awaitNanos(nanos);
            }
            x = dequeue();
            c = count.getAndAdd(-x.getByteCount());
            if (c > 1)
                notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
        if (c == maxBytesCount)
            signalNotFull();
        return x;
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return 0;
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        return 0;
    }

    @Override
    public E poll() {
        return null;
    }

    @Override
    public E peek() {
        return null;
    }

    /**
     * Unlinks interior Node p with predecessor trail.
     */
    void unlink(LimitBytesBlockingQueue.Node<E> p, LimitBytesBlockingQueue.Node<E> trail) {
        // assert isFullyLocked();
        // p.next is not changed, to allow iterators that are
        // traversing p to maintain their weak-consistency guarantee.
        p.item = null;
        trail.next = p.next;
        if (last == p)
            last = trail;
        if (count.getAndDecrement() == maxBytesCount)
            notFull.signal();
    }


    /**
     * Returns an iterator over the elements in this queue in proper sequence.
     * The elements will be returned in order from first (head) to last (tail).
     * <p>
     * <p>The returned iterator is
     * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     *
     * @return an iterator over the elements in this queue in proper sequence
     */
    public Iterator<E> iterator() {
        return new LimitBytesBlockingQueue.Itr();
    }

    private class Itr implements Iterator<E> {
        /*
         * Basic weakly-consistent iterator.  At all times hold the next
         * item to hand out so that if hasNext() reports true, we will
         * still have it to return even if lost race with a take etc.
         */

        private LimitBytesBlockingQueue.Node<E> current;
        private LimitBytesBlockingQueue.Node<E> lastRet;
        private E currentElement;

        Itr() {
            fullyLock();
            try {
                current = head.next;
                if (current != null)
                    currentElement = current.item;
            } finally {
                fullyUnlock();
            }
        }

        public boolean hasNext() {
            return current != null;
        }

        /**
         * Returns the next live successor of p, or null if no such.
         * <p>
         * Unlike other traversal methods, iterators need to handle both:
         * - dequeued nodes (p.next == p)
         * - (possibly multiple) interior removed nodes (p.item == null)
         */
        private LimitBytesBlockingQueue.Node<E> nextNode(LimitBytesBlockingQueue.Node<E> p) {
            for (; ; ) {
                LimitBytesBlockingQueue.Node<E> s = p.next;
                if (s == p)
                    return head.next;
                if (s == null || s.item != null)
                    return s;
                p = s;
            }
        }

        public E next() {
            fullyLock();
            try {
                if (current == null)
                    throw new NoSuchElementException();
                E x = currentElement;
                lastRet = current;
                current = nextNode(current);
                currentElement = (current == null) ? null : current.item;
                return x;
            } finally {
                fullyUnlock();
            }
        }

        public void remove() {
            if (lastRet == null)
                throw new IllegalStateException();
            fullyLock();
            try {
                LimitBytesBlockingQueue.Node<E> node = lastRet;
                lastRet = null;
                for (LimitBytesBlockingQueue.Node<E> trail = head, p = trail.next;
                     p != null;
                     trail = p, p = p.next) {
                    if (p == node) {
                        unlink(p, trail);
                        break;
                    }
                }
            } finally {
                fullyUnlock();
            }
        }
    }

}
