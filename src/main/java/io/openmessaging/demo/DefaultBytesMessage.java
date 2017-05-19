package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.util.Arrays;

public class DefaultBytesMessage implements BytesMessage {

    private static MessageEncoder encoder = new PBMessageEncoder();

    private KeyValue headers = new DefaultKeyValue();
    private KeyValue properties;
    private byte[] body;

    /**
     * 序列化之后的字节数
     */
    private int byteCount;

    public DefaultBytesMessage(byte[] body) {
        this.body = body;
    }

    public void setHeaders(KeyValue headers) {
        this.headers = headers;
    }

    @Override
    public byte[] getBody() {
        return body;
    }

    public void setProperties(KeyValue properties) {
        this.properties = properties;
    }

    @Override
    public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }

    @Override
    public KeyValue headers() {
        return headers;
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, int value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, long value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, double value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, String value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override
    public String toString() {
        return "DefaultBytesMessage{" +
                "headers=" + headers +
                ", properties=" + properties +
                ", body=" + new String(body) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DefaultBytesMessage that = (DefaultBytesMessage) o;

        if (!headers.equals(that.headers)) return false;
        if (properties != null ? !properties.equals(that.properties) : that.properties != null) return false;
        return Arrays.equals(body, that.body);

    }

    @Override
    public int hashCode() {
        int result = headers.hashCode();
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(body);
        return result;
    }

    public int getByteCount() {
        return byteCount;
    }

    public void setByteCount(int byteCount) {
        this.byteCount = byteCount;
    }


    byte[] serializeBytes = null;

    /**
     * 序列化自己
     *
     * @return
     */
    public byte[] pbSerialize() {
        serializeBytes = encoder.message2Bytes(this);
        byteCount = serializeBytes.length;
        return serializeBytes;
    }

    public byte[] getSerializeBytes() {
        return serializeBytes;
    }

    public void setSerializeBytes(byte[] serializeBytes) {
        this.serializeBytes = serializeBytes;
    }

    /**
     * 清空信息
     */
    public void clear() {
        ((DefaultKeyValue) headers).clear();
        if (properties != null)
            ((DefaultKeyValue) properties).clear();
        body = null;
        serializeBytes = null;
        byteCount = 0;
    }
}
