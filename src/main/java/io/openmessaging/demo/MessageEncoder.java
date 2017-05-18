package io.openmessaging.demo;

/**
 * Created by Xingfeng on 2017-05-15.
 */
public interface MessageEncoder {

    byte[] message2Bytes(DefaultBytesMessage message);

}
