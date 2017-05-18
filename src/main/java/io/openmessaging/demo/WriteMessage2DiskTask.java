package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.*;
import java.util.concurrent.BlockingQueue;

/**
 * 将消息写入到磁盘的任务，一个topic或一个queue对应一个任务
 * Created by Xingfeng on 2017-05-15.
 */
public class WriteMessage2DiskTask implements Runnable {

    private String parent;
    private String fileName;
    private BlockingQueue<Message> queue;

    private MessageEncoder messageEncoder = new PBMessageEncoder();

    private File file;
    private FileOutputStream out;

    public WriteMessage2DiskTask(String parent, String fileName, BlockingQueue<Message> queue) {
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

            if (file.exists()) {

                out = new FileOutputStream(file, true);

                try {
                    Message message = null;
                    byte[] data = null;
                    while ((message = queue.take()) != null) {
                        data = messageEncoder.message2Bytes((DefaultBytesMessage) message);
                        out.write(data);
//                        out.flush();
                    }


                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    if (out != null)
                        out.close();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
