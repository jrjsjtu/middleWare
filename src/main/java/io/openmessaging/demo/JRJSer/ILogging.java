package io.openmessaging.demo.JRJSer;

/**
 * Created by jrj on 17-6-2.
 */
//this is a interface for Logging
public interface ILogging {
    void append(byte[] byteArray,int len);
    void signalFlush();

}
