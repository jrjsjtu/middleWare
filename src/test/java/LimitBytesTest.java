import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.LimitBytesBlockingQueue;

/**
 * Created by Xingfeng on 2017-05-19.
 */
public class LimitBytesTest {

    static class Producer implements Runnable {

        private LimitBytesBlockingQueue blockingQueue;

        public Producer(LimitBytesBlockingQueue blockingQueue) {
            this.blockingQueue = blockingQueue;
        }

        @Override
        public void run() {

            int num = 0;
            DefaultBytesMessage message = new DefaultBytesMessage(null);
            message.setByteCount(10);
            while (num++ < 10) {
                try {
                    blockingQueue.put(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    static class Consumer implements Runnable {

        private LimitBytesBlockingQueue blockingQueue;

        public Consumer(LimitBytesBlockingQueue blockingQueue) {
            this.blockingQueue = blockingQueue;
        }

        @Override
        public void run() {

            int num = 0;
            DefaultBytesMessage message = new DefaultBytesMessage(null);
            try {
                while ((message = blockingQueue.take()) != null) {
                    System.out.println(message.getByteCount());
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) throws InterruptedException {

        //设置容量为20byte
        LimitBytesBlockingQueue blockingQueue = new LimitBytesBlockingQueue(20);

        new Thread(new Producer(blockingQueue)).start();
        new Thread(new Consumer(blockingQueue)).start();


    }

}
