package io.openmessaging.demo;

/**
 * Created by Xingfeng on 2017-05-19.
 */
public class Constant {

    /**
     * 扇区大小，512b
     */
    public static final int SECTOR_SIZE = 512;

    /**
     * 消息的最大尺寸，256K
     */
    public static final int MGS_MAX_SIZE = 256 * 1024;

    /**
     * 消息回收池的大小，50MB
     */
    public static final int MESSAGE_POOL_SIZE = 50 * 1024 * 1024;

    /**
     * 每个阻塞队列的最大字节数，15MB
     */
    public static final int MAX_BYTES_COUNT = 15 * 1024 * 1204;

    /**
     * 每页大小，4K
     */
    public static final int PAGE_SIZE = 4 * 1024;
}
