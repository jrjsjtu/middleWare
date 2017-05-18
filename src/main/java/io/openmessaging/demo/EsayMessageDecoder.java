package io.openmessaging.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Xingfeng on 2017-05-15.
 */
public class EsayMessageDecoder implements MessageDecoder {

    @Override
    public DefaultBytesMessage bytes2Message(InputStream in) {

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        DefaultBytesMessage defaultBytesMessage = null;
        try {
            while ((line = reader.readLine()) != null) {
                //headers的kv对
                DefaultKeyValue headers = DefaultKeyValue.parseKVLine(line);

                System.out.println(line);

                //properties的kv对
                line = reader.readLine().trim();
                DefaultKeyValue properties = DefaultKeyValue.parseKVLine(line);

                //body部分的长度
                int bodyLength = Integer.parseInt(reader.readLine());
                line = reader.readLine();
                byte[] body = line.getBytes();

                //组装消息
                defaultBytesMessage = new DefaultBytesMessage(body);
                defaultBytesMessage.setHeaders(headers);
                defaultBytesMessage.setProperties(properties);
                return defaultBytesMessage;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
