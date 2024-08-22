package xiaolaa.main.mintdb;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class UDPServer {
    public static void main(String[] args) throws IOException, InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(4);
        new Thread(() -> {
            try {
                countDownLatch.await();
                System.out.println(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }).start();

        new Thread(() -> {
            try {
                countDownLatch.await();
                System.out.println(2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        for (int i = 0; i < 4; i++) {
            new Thread(() -> {
                System.out.println(Thread.currentThread().getName());
                countDownLatch.countDown();

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }).start();
        }
        countDownLatch.await();
        System.out.println("Main Thread");


    }

}
