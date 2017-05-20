package io.openmessaging.demo;

/**
 * 对生产的消息进行分发
 * Created by Xingfeng on 2017-05-20.
 */
public class Dispatcher {

    private static Dispatcher instance = null;



    private Dispatcher() {
    }

    public static Dispatcher getInstance() {
        if (instance == null) {
            synchronized (Dispatcher.class) {
                if (instance == null)
                    instance = new Dispatcher();
            }
        }
        return instance;
    }

}
