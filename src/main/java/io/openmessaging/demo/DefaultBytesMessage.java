package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.util.Arrays;

public class DefaultBytesMessage implements BytesMessage {

    private static final String KEY_VALUE_SPLT = ":";
    private static final String ENTRY_VALUE = " ";

    private KeyValue headers = new DefaultKeyValue();
    private KeyValue properties;

    private StringBuilder headersLine = new StringBuilder();
    private StringBuilder propertiesLine = new StringBuilder();

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

    public String getHeadersLine() {
        return headersLine.toString();
    }

    public String getPropertiesLine() {
        return propertiesLine.toString();
    }

    @Override
    public byte[] getBody() {
        return body;
    }

    public void setProperties(KeyValue properties) {
        this.properties = properties;
        if (properties != null)
            this.propertiesLine = initPropertiesLine((DefaultKeyValue) properties);
    }

    public StringBuilder initPropertiesLine(DefaultKeyValue defaultKeyValue) {

        StringBuilder sb = new StringBuilder();
        DefaultKeyValue.NewValue newValue = null;

        for (String key : defaultKeyValue.keySet()) {
            newValue = defaultKeyValue.getNewValue(key);
            if (newValue == null) {
                continue;
            } else {
                sb.append(key + KEY_VALUE_SPLT + newValue.value + KEY_VALUE_SPLT + newValue.type).append(ENTRY_VALUE);
            }
        }

        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }

        return sb;
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
        headersLine.append(key + KEY_VALUE_SPLT + value + KEY_VALUE_SPLT + 0);
        return this;
    }

    @Override
    public Message putHeaders(String key, long value) {
        headers.put(key, value);
        headersLine.append(key + KEY_VALUE_SPLT + value + KEY_VALUE_SPLT + 1);
        return this;
    }

    @Override
    public Message putHeaders(String key, double value) {
        headers.put(key, value);
        headersLine.append(key + KEY_VALUE_SPLT + value + KEY_VALUE_SPLT + 2);
        return this;
    }

    @Override
    public Message putHeaders(String key, String value) {
        headers.put(key, value);
        headersLine.append(key + KEY_VALUE_SPLT + value + KEY_VALUE_SPLT + 3);
        return this;
    }

    @Override
    public Message putProperties(String key, int value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        propertiesLine.append(key + KEY_VALUE_SPLT + value + KEY_VALUE_SPLT + 0);
        return this;
    }

    @Override
    public Message putProperties(String key, long value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        propertiesLine.append(key + KEY_VALUE_SPLT + value + KEY_VALUE_SPLT + 1);
        return this;
    }

    @Override
    public Message putProperties(String key, double value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        propertiesLine.append(key + KEY_VALUE_SPLT + value + KEY_VALUE_SPLT + 2);

        return this;
    }

    @Override
    public Message putProperties(String key, String value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        propertiesLine.append(key + KEY_VALUE_SPLT + value + KEY_VALUE_SPLT + 3);
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
}
