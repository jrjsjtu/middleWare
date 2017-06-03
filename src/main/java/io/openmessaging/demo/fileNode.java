package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageHeader;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by jrj on 17-5-30.
 */
public class fileNode extends Thread{
    int len;
    byte[] body;char tmp;
    int strlen;byte[] tmpkey,tmpvalue;String key,valuestr;
    int headerInt;long headerLong;double headerDouble;
    private int pageSize = 8*1024*1024;//pagesize太小反而会OOM
    ByteBuffer curByteBuffer = null;
    long fileSize;
    long curPostion = 0;

    RandomAccessFile raf = null;

    byte[] byte4message = new byte[9*1024*1024];//为了应对大的message提前开好512K的缓存
    public fileNode(String fileName){
        try {
            raf = new RandomAccessFile (fileName, "r");
            fileSize = raf.length();
            if (fileSize<= pageSize){
                pageSize = (int)fileSize;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        getByteBuffer();
    }

    private void getByteBuffer(){
        try {
            raf.read(byte4message,0,pageSize);
            //raf.read(byte4int);
            curPostion += pageSize;
            curByteBuffer = ByteBuffer.wrap(byte4message,0,pageSize);
            //System.out.println("first time we are at position " + curPostion);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean getLargerByteBuffer() {
        if (curPostion == fileSize){
            return false;
        }
        long sizeThisTime;
        if (curPostion+pageSize>fileSize){
            sizeThisTime = fileSize-curPostion;
        }else{
            sizeThisTime = pageSize;
        }
        int i=0;
        while(curByteBuffer.hasRemaining()){
            byte4message[i] = curByteBuffer.get();
            i++;
        }
        try{
            raf.read(byte4message,i,(int)sizeThisTime);
        }catch(Exception e){
            System.out.println(i + "  "+ sizeThisTime+" "+ len);
            e.printStackTrace();
        }
        curByteBuffer = ByteBuffer.wrap(byte4message,0,i+(int)sizeThisTime);
        curPostion += sizeThisTime;
        return true;
    }
    private BytesMessage getMessageList(){
        OutputMesssage message = null;
        if(curByteBuffer.remaining()<4) {
            if (getLargerByteBuffer()==false){
                return null;
            }
        }
        len = curByteBuffer.getInt();
        while (curByteBuffer.remaining() < len) {
            getLargerByteBuffer();
        }
        body = new byte[len];
        curByteBuffer.get(body);
        message = new OutputMesssage(body);

        while (true){
            if (curByteBuffer.remaining()<2) {
                getLargerByteBuffer();
            }
            try{
                tmp = curByteBuffer.getChar();
            }catch (Exception e){
                System.out.println(len);
                e.printStackTrace();
            }

            if (tmp == ' ') {break;}
            switch (tmp){
                case '1':
                    if (curByteBuffer.remaining()<8) {
                        getLargerByteBuffer();
                    }
                    headerInt = curByteBuffer.getInt();
                    strlen = curByteBuffer.getInt();
                    while (curByteBuffer.remaining() < strlen) {
                        getLargerByteBuffer();
                    }
                    tmpkey = new byte[strlen];
                    curByteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putHeaders(key,headerInt);break;
                case '2':
                    if (curByteBuffer.remaining()<8+4) {
                        getLargerByteBuffer();
                    }
                    headerLong = curByteBuffer.getLong();
                    strlen = curByteBuffer.getInt();
                    while (curByteBuffer.remaining() < strlen) {
                        getLargerByteBuffer();
                    }
                    tmpkey = new byte[strlen];
                    curByteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putHeaders(key,headerLong);break;
                case '3':
                    if (curByteBuffer.remaining()<8+4) {
                        getLargerByteBuffer();
                    }
                    headerDouble = curByteBuffer.getDouble();
                    strlen = curByteBuffer.getInt();
                    while (curByteBuffer.remaining() < strlen) {
                        getLargerByteBuffer();
                    }
                    tmpkey = new byte[strlen];
                    curByteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putHeaders(key,headerDouble);break;
                case '4':
                    if (curByteBuffer.remaining()<4) {
                        getLargerByteBuffer();
                    }
                    strlen = curByteBuffer.getInt();
                    while (curByteBuffer.remaining() < strlen) {
                        getLargerByteBuffer();
                    }
                    tmpvalue = new byte[strlen];
                    curByteBuffer.get(tmpvalue);
                    valuestr = new String(tmpvalue);
                    if (curByteBuffer.remaining()<4) {
                        getLargerByteBuffer();
                    }
                    strlen = curByteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    while (curByteBuffer.remaining() < strlen) {
                        getLargerByteBuffer();
                    }
                    curByteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putHeaders(key,valuestr);break;
            }
        }

        while (true){
            if (curByteBuffer.remaining()<2) {
                getLargerByteBuffer();
            }
            tmp = curByteBuffer.getChar();
            if (tmp == ' ') {break;}
            switch (tmp){
                case '1':
                    if (curByteBuffer.remaining()<8) {
                        getLargerByteBuffer();
                    }
                    headerInt = curByteBuffer.getInt();
                    strlen = curByteBuffer.getInt();
                    while (curByteBuffer.remaining() < strlen) {
                        getLargerByteBuffer();
                    }
                    tmpkey = new byte[strlen];
                    curByteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putProperties(key,headerInt);break;
                case '2':
                    if (curByteBuffer.remaining()<8+4) {
                        getLargerByteBuffer();
                    }
                    headerLong = curByteBuffer.getLong();
                    strlen = curByteBuffer.getInt();
                    while (curByteBuffer.remaining() < strlen) {
                        getLargerByteBuffer();
                    }
                    tmpkey = new byte[strlen];
                    curByteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putProperties(key,headerLong);break;
                case '3':
                    if (curByteBuffer.remaining()<8+4) {
                        getLargerByteBuffer();
                    }
                    headerDouble = curByteBuffer.getDouble();
                    strlen = curByteBuffer.getInt();
                    while (curByteBuffer.remaining() < strlen) {
                        getLargerByteBuffer();
                    }
                    tmpkey = new byte[strlen];
                    curByteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putProperties(key,headerDouble);break;
                case '4':
                    if (curByteBuffer.remaining()<4) {
                        getLargerByteBuffer();
                    }
                    strlen = curByteBuffer.getInt();
                    while (curByteBuffer.remaining() < strlen) {
                        getLargerByteBuffer();
                    }
                    tmpvalue = new byte[strlen];
                    curByteBuffer.get(tmpvalue);
                    valuestr = new String(tmpvalue);
                    if (curByteBuffer.remaining()<4) {
                        getLargerByteBuffer();
                    }
                    strlen = curByteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    while (curByteBuffer.remaining() < strlen) {
                        getLargerByteBuffer();
                    }
                    curByteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putProperties(key,valuestr);break;
            }
        }

        return message;
    }

    String topic;
    public void run(){
        ArrayList interestArray;
        while (true){
            BytesMessage message = getMessageList();
            if (message == null){
                break;
            }
            topic = message.headers().getString(MessageHeader.TOPIC);
            if(topic == null){
                topic = message.headers().getString(MessageHeader.QUEUE);
            }
            interestArray = DefaultPullConsumer.interestList.get(topic);
            if (interestArray != null){
                for (Object pullConsumer: interestArray){
                    try {
                        ((DefaultPullConsumer)pullConsumer).msgQueue.put(message);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        if (DefaultPullConsumer.remainThread.decrementAndGet() == 0){
            OutputMesssage endMessage = new OutputMesssage(new byte[1123]);
            for (DefaultPullConsumer pullConsumer:DefaultPullConsumer.notifyList){
                try {
                    pullConsumer.msgQueue.put(endMessage);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}