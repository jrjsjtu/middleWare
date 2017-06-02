package io.openmessaging.demo.JRJSer;

import java.util.concurrent.CountDownLatch;

/**
 * Created by jrj on 17-6-2.
 */
public class AbstractLogging implements ILogging,Runnable {
    public static CountDownLatch endSignal;
    public static final int fileMagicNumber = 17770;
    @Override
    public void append(byte[] byteArray, int len) {

    }

    @Override
    public void signalFlush() {

    }

    @Override
    public void run() {

    }
}
