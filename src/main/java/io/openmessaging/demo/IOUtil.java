package io.openmessaging.demo;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by Xingfeng on 2017-05-18.
 */
public class IOUtil {

    public static void close(Closeable closeable) {
        try {
            if (closeable != null)
                closeable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
