package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.demo.JRJSer.typeStruct;
import java.util.Arrays;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DefaultBytesMessage implements BytesMessage {

    KeyValue headers = new DefaultKeyValue();
    KeyValue properties = new DefaultKeyValue();
    private HashMap<String,byte[]> headerMap = null;
    private HashMap<String,byte[]> propertiesMap = null;
    int size = 0;
    private byte[] body;

    public DefaultBytesMessage(byte[] body) {
        this.body = body;
    }

    public void setProperties(KeyValue properties){

    }
    @Override public byte[] getBody() {
        return body;
    }

    @Override public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }

    @Override public KeyValue headers() {
        return headers;
    }

    @Override public KeyValue properties() {
        return properties;
    }


    @Override public Message putHeaders(String key, int value) {
        if (headerMap == null) headerMap = new HashMap<>(4);
        byte[] tmp = typeStruct.getByteArray(value,key);
        size += tmp.length;
        headerMap.put(key, tmp);
        headers.put(key,value);
        return this;
    }

    @Override public Message putHeaders(String key, long value) {
        if (headerMap == null) headerMap = new HashMap(4);
        byte[] tmp = typeStruct.getByteArray(value,key);
        size += tmp.length;
        headerMap.put(key, tmp);
        headers.put(key,value);
        return this;
    }

    @Override public Message putHeaders(String key, double value) {
        if (headerMap == null) headerMap = new HashMap<>(4);
        byte[] tmp = typeStruct.getByteArray(value,key);
        size += tmp.length;
        headerMap.put(key, tmp);
        headers.put(key,value);
        return this;
    }

    @Override public Message putHeaders(String key, String value) {
        if (headerMap == null) headerMap = new HashMap<>(4);
        byte[] tmp = typeStruct.getByteArray(value,key);
        size += tmp.length;
        headerMap.put(key, tmp);
        headers.put(key,value);
        return this;
    }

    @Override public Message putProperties(String key, int value) {
        if (propertiesMap == null) propertiesMap = new HashMap<>(4);
        byte[] tmp = typeStruct.getByteArray(value,key);
        size += tmp.length;
        propertiesMap.put(key, tmp);
        properties.put(key,value);
        return this;
    }

    @Override public Message putProperties(String key, long value) {
        if (propertiesMap == null) propertiesMap = new HashMap<>(4);
        byte[] tmp = typeStruct.getByteArray(value,key);
        size += tmp.length;
        propertiesMap.put(key, tmp);
        properties.put(key,value);
        return this;
    }

    @Override public Message putProperties(String key, double value) {
        if (propertiesMap == null) propertiesMap = new HashMap<>(4);
        byte[] tmp = typeStruct.getByteArray(value,key);
        size += tmp.length;
        propertiesMap.put(key, tmp);
        properties.put(key,value);
        return this;
    }

    @Override public Message putProperties(String key, String value) {
        if (propertiesMap == null) propertiesMap = new HashMap<>(4);
        byte[] tmp = typeStruct.getByteArray(value,key);
        size += tmp.length;
        propertiesMap.put(key, tmp);
        properties.put(key,value);
        return this;
    }

    public byte[] getByteArray(){
        ByteBuffer byteBuffer = ByteBuffer.allocate(body.length+size+2+4+2);
        byteBuffer.putInt(body.length);
        byteBuffer.put(body);
        Iterator iter;
        if (headerMap != null) {
            iter = headerMap.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                byteBuffer.put((byte[]) entry.getValue());
            }
        }
        byteBuffer.putChar(' ');
        if (propertiesMap != null) {
            iter = propertiesMap.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                byteBuffer.put((byte[]) entry.getValue());
            }
        }
        byteBuffer.putChar(' ');
        return byteBuffer.array();
    }

    public int getByteCount(){
        return 0;
    }
    public void clear(){}
    public void setByteCount(int byteCount){
    }
}