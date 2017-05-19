package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;

/**
 * 将消息写入到Topic，辅助以索引文件，文件格式为 position size ，两个均为int值
 * Created by Xingfeng on 2017-05-15.
 */
public class WriteTopicTask implements Runnable {

    private String parent;
    private String fileName;
    private LimitBytesBlockingQueue<DefaultBytesMessage> queue;

    private MessageEncoder messageEncoder = new PBMessageEncoder();

    private File file;
    private FileOutputStream out;

    //索引文件
    private RandomAccessFile indexFile;
    private FileChannel indexChannel;

    public WriteTopicTask(String parent, String fileName, LimitBytesBlockingQueue<DefaultBytesMessage> queue) {
        this.parent = parent;
        this.fileName = fileName;
        this.queue = queue;
    }

    @Override
    public void run() {


        try {

            file = new File(parent, fileName);
            if (!file.exists()) {
                file.createNewFile();

            } else {
                file.delete();
                file.createNewFile();
            }

            indexFile = new RandomAccessFile(new File(parent, fileName + "_index"), "rw");

            if (file.exists()) {

                out = new FileOutputStream(file, true);
                indexChannel = indexFile.getChannel();

                ByteBuffer buffer = ByteBuffer.allocate(8);
                int position = 0, size = 0;
                try {
                    DefaultBytesMessage message = null;
                    byte[] data = null;
                    while ((message = queue.take()) != null) {
                        data = message.getSerializeBytes();

                        //写索引
                        size = data.length;
                        buffer.position(0);
                        buffer.putInt(position);
                        buffer.putInt(size);
                        position += size;
                        buffer.rewind();
                        indexChannel.write(buffer);

                        //写消息
                        out.write(data);

                        //回收Message
                        MessagePool.recycle((DefaultBytesMessage) message);

                    }


                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    IOUtil.close(out);
                    IOUtil.close(indexChannel);

                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
