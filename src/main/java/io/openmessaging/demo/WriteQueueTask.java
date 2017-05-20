package io.openmessaging.demo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 将消息写入到磁盘的任务，一个topic或一个queue对应一个任务
 * Created by Xingfeng on 2017-05-15.
 */
public class WriteQueueTask implements Runnable {

    private String parent;
    private String fileName;
    private LimitBytesBlockingQueue<DefaultBytesMessage> queue;

    private RandomAccessFile file;
    private FileChannel fileChannel;

    public WriteQueueTask(String parent, String fileName, LimitBytesBlockingQueue<DefaultBytesMessage> queue) {
        this.parent = parent;
        this.fileName = fileName;
        this.queue = queue;
    }

    @Override
    public void run() {

        try {

            long offset = 0;
            file = new RandomAccessFile(new File(parent, fileName), "rw");
            fileChannel = file.getChannel();
            MappedByteBuffer buffer = buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, offset, Constant.PAGE_SIZE);
            try {
                DefaultBytesMessage message = null;
                byte[] data = null;
                while ((message = queue.take()) != null) {
                    data = message.getSerializeBytes();

                    int start = 0;
                    while (true) {

                        while (start < data.length && buffer.hasRemaining()) {
                            buffer.put(data[start++]);
                        }
                        if (start >= data.length) {
                            break;
                        }
                        //Buffer满了
                        else {
                            offset += Constant.PAGE_SIZE;
                            //说明满了
                            buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, offset, Constant.PAGE_SIZE);
                        }
                    }

                    //回收Message
                    MessagePool.recycle(message);

                    Thread.yield();

                }


            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                IOUtil.close(file);
                IOUtil.close(fileChannel);

            }


        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
