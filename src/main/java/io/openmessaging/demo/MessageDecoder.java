package io.openmessaging.demo;

import java.io.InputStream;

/**
 * Created by Xingfeng on 2017-05-15.
 */
public interface MessageDecoder {

    DefaultBytesMessage bytes2Message(InputStream in);

}
