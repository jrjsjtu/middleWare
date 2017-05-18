package io.openmessaging.demo;

import io.openmessaging.KeyValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DefaultKeyValue implements KeyValue {

    //k:v:t之间的分隔符
    private static final String ENTRY_SPLIT = " ";
    //k v t之间的分隔符
    private static final String K_V_T_SPLIT = ":";

    public static class NewValue {

        //0表示int，1表示long、2表示double、3表示string
        byte type;
        Object value;

        int intValue;
        long longValue;
        double doubleValue;
        String stringValue;


        public NewValue(byte type, int intValue) {
            this.type = type;
            this.intValue = intValue;
            this.value = intValue;
        }

        public NewValue(byte type, long longValue) {
            this.type = type;
            this.longValue = longValue;
            this.value = longValue;
        }

        public NewValue(byte type, double doubleValue) {
            this.type = type;
            this.doubleValue = doubleValue;
            this.value = doubleValue;
        }

        public NewValue(byte type, String stringValue) {
            this.type = type;
            this.stringValue = stringValue;
            this.value = stringValue;
        }

        public int getIntValue() {
            return intValue;
        }

        public long getLongValue() {
            return longValue;
        }

        public double getDoubleValue() {
            return doubleValue;
        }

        public String getStringValue() {
            return stringValue;
        }

        @Override
        public String toString() {
            return "NewValue{" +
                    "value=" + value +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NewValue newValue = (NewValue) o;

            if (type != newValue.type) return false;
            return value != null ? value.equals(newValue.value) : newValue.value == null;

        }

        @Override
        public int hashCode() {
            int result = (int) type;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }
    }

    private final Map<String, NewValue> kvs = new HashMap<>();

    @Override
    public KeyValue put(String key, int value) {
        kvs.put(key, new NewValue((byte) 0, value));
        return this;
    }

    @Override
    public KeyValue put(String key, long value) {
        kvs.put(key, new NewValue((byte) 1, value));
        return this;
    }

    @Override
    public KeyValue put(String key, double value) {
        kvs.put(key, new NewValue((byte) 2, value));
        return this;
    }

    @Override
    public KeyValue put(String key, String value) {
        kvs.put(key, new NewValue((byte) 3, value));
        return this;
    }

    public NewValue getNewValue(String key) {
        return kvs.getOrDefault(key, null);
    }

    @Override
    public int getInt(String key) {
        NewValue newValue = kvs.getOrDefault(key, new NewValue((byte) 0, 0));
        return newValue.getIntValue();
    }

    @Override
    public long getLong(String key) {
        NewValue newValue = kvs.getOrDefault(key, new NewValue((byte) 1, 0L));
        return newValue.getLongValue();
    }

    @Override
    public double getDouble(String key) {
        NewValue newValue = kvs.getOrDefault(key, new NewValue((byte) 2, 0.0d));
        return newValue.getDoubleValue();
    }

    @Override
    public String getString(String key) {
        NewValue newValue = kvs.getOrDefault(key, new NewValue((byte) 3, null));
        return newValue.getStringValue();
    }

    public NewValue get(String key) {
        return kvs.getOrDefault(key, null);
    }

    @Override
    public Set<String> keySet() {
        return kvs.keySet();
    }

    @Override
    public boolean containsKey(String key) {
        return kvs.containsKey(key);
    }

    @Override
    public String toString() {
        return "kvs=" + kvs.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DefaultKeyValue that = (DefaultKeyValue) o;

        return kvs != null ? kvs.equals(that.kvs) : that.kvs == null;

    }

    @Override
    public int hashCode() {
        return kvs != null ? kvs.hashCode() : 0;
    }

    public static DefaultKeyValue parseKVLine(String line) {

        if (line.trim().equals("")) {
            return null;
        }

        DefaultKeyValue result = new DefaultKeyValue();
        String[] array = line.split(ENTRY_SPLIT);
        String[] kvts = null;
        for (String kvt : array) {

            kvts = kvt.split(K_V_T_SPLIT);
            String key = kvts[0];
            int type = Integer.parseInt(kvts[2]);
            switch (type) {
                case 0:
                    result.put(key, Integer.parseInt(kvts[1]));
                    break;
                case 1:
                    result.put(key, Long.parseLong(kvts[1]));
                    break;
                case 2:
                    result.put(key, Double.parseDouble(kvts[1]));
                    break;
                case 3:
                    result.put(key, kvts[1]);
                    break;
            }
        }
        return result;
    }
}
