package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by jrj on 17-5-30.
 */
public class fileNode {
    int len;
    byte[] body;char tmp;
    int strlen;byte[] tmpkey,tmpvalue;String key,valuestr;
    int headerInt;long headerLong;double headerDouble;
    private int pageSize = 1024*4;
    ByteBuffer curByteBuffer = null;
    long fileSize;
    long curPostion = 0;

    FileChannel fc;
    MappedByteBuffer mbb;
    byte[] byte4message = new byte[1024*1024];//为了应对大的message提前开好512K的缓存
    public fileNode(String fileName){
        try {
            fc = new RandomAccessFile (fileName, "r").getChannel();
            //raf = new FileInputStream(fileName);
            fileSize = fc.size();
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
            mbb = fc.map(FileChannel.MapMode.READ_ONLY,curPostion,pageSize);
            //raf.read(byte4int);
            curPostion += pageSize;
            mbb.get(byte4message,0,pageSize);
            curByteBuffer = ByteBuffer.wrap(byte4message,0,pageSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Message getOneMessage(){
        return getMessageList();
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
        try {
            mbb = fc.map(FileChannel.MapMode.READ_ONLY,curPostion,sizeThisTime);
        } catch (IOException e) {
            System.out.println(curPostion);
            System.out.println(pageSize);
            System.out.println(fileSize);
            e.printStackTrace();
        }
        int i=0;
        while (curByteBuffer.hasRemaining()){
            byte4message[i] = curByteBuffer.get();
            i++;
        }
        try{
            mbb.get(byte4message,i,(int)sizeThisTime);
        }catch(Exception e){
            System.out.println(i);
            System.out.println(sizeThisTime);
            e.printStackTrace();
        }
        curByteBuffer =ByteBuffer.wrap(byte4message,0,i+(int)sizeThisTime);
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
        while (curByteBuffer.remaining() <= len) {
            getLargerByteBuffer();
        }
        body = new byte[len];
        curByteBuffer.get(body);
        message = new OutputMesssage(body);
        while (true){
            if (curByteBuffer.remaining()<2) {
                getLargerByteBuffer();
            }
            tmp = curByteBuffer.getChar();
            if (tmp == ' ') break;
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
            if (tmp == ' ') break;
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
    public void closeFileFD(){
        try {
            fc.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
