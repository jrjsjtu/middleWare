package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.Inflater;

/**
 * Created by jrj on 17-5-30.
 */
public class fileNode {
    int len;
    byte[] body;char tmp;
    int strlen;byte[] tmpkey,tmpvalue;String key,valuestr;
    int headerInt;long headerLong;double headerDouble;
    ByteBuffer curByteBuffer = null;
    long fileSize;
    long curPostion = 0;

    RandomAccessFile raf = null;
    Inflater decompresser = new Inflater();
    byte[] byte4message = new byte[2*1024*1024];//为了应对大的message提前开好512K的缓存
    byte[] byte4zip = new byte[1024*1024];
    byte[] byte4int = new byte[4];
    public fileNode(String fileName){
        try {
            raf = new RandomAccessFile (fileName, "r");
            fileSize = raf.length();
        } catch (Exception e) {
            e.printStackTrace();
        }
        getByteBuffer();
    }

    private boolean getByteBuffer(){
        if (curPostion == fileSize){
            return false;
        }
        try {
            //raf.read(byte4message,0,pageSize);
            raf.read(byte4int);
            int len = ByteBuffer.wrap(byte4int).getInt();
            raf.read(byte4zip,0,len);
            decompresser.setInput(byte4zip, 0, len);
            int resultLength = decompresser.inflate(byte4message);
            decompresser.reset();
            curByteBuffer = ByteBuffer.wrap(byte4message,0,resultLength);
            curPostion += (4+len);
            //System.out.println("first time we are at position " + curPostion);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    public Message getOneMessage(){
        return getMessageList();
    }

    private BytesMessage getMessageList(){
        OutputMesssage message = null;
        if(curByteBuffer.remaining() == 0) {
            if (getByteBuffer()==false){
                return null;
            }
        }
        len = curByteBuffer.getInt();
        body = new byte[len];
        curByteBuffer.get(body);
        message = new OutputMesssage(body);

        while (true){
            try{
                tmp = curByteBuffer.getChar();
            }catch (Exception e){
                System.out.println(len);
                e.printStackTrace();
            }
            if (tmp == ' ') {break;}
            switch (tmp){
                case '1':
                    headerInt = curByteBuffer.getInt();
                    strlen = curByteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    curByteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putHeaders(key,headerInt);break;
                case '2':
                    headerLong = curByteBuffer.getLong();
                    strlen = curByteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    curByteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putHeaders(key,headerLong);break;
                case '3':
                    headerDouble = curByteBuffer.getDouble();
                    strlen = curByteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    curByteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putHeaders(key,headerDouble);break;
                case '4':
                    strlen = curByteBuffer.getInt();
                    tmpvalue = new byte[strlen];
                    curByteBuffer.get(tmpvalue);
                    valuestr = new String(tmpvalue);
                    strlen = curByteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    curByteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putHeaders(key,valuestr);break;
            }
        }

        while (true){
            tmp = curByteBuffer.getChar();
            if (tmp == ' ') {break;}
            switch (tmp){
                case '1':
                    headerInt = curByteBuffer.getInt();
                    strlen = curByteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    curByteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putProperties(key,headerInt);break;
                case '2':
                    headerLong = curByteBuffer.getLong();
                    strlen = curByteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    curByteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putProperties(key,headerLong);break;
                case '3':
                    headerDouble = curByteBuffer.getDouble();
                    strlen = curByteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    curByteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putProperties(key,headerDouble);break;
                case '4':
                    strlen = curByteBuffer.getInt();
                    tmpvalue = new byte[strlen];
                    curByteBuffer.get(tmpvalue);
                    valuestr = new String(tmpvalue);
                    strlen = curByteBuffer.getInt();
                    tmpkey = new byte[strlen];
                    curByteBuffer.get(tmpkey);
                    key = new String(tmpkey);
                    message.putProperties(key,valuestr);break;
            }
        }

        return message;
    }
    public void closeFileFD(){
        try {
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}