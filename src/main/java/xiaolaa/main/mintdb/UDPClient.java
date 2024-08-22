package xiaolaa.main.mintdb;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

class MyArray{
    int[] array;
    int index;
    int size = 0;

    Iterator<Integer> iterator = iterator();

    public Iterator<Integer> iterator() {
        return new MyIterator();
    }

    void forEach(Consumer<Integer> action) {
        while (iterator.hasNext()) {
            action.accept(iterator.next());
        }
    }

    MyArray() {
        array = new int[16];
        index = 0;
    }

    class MyIterator implements Iterator<Integer> {
        int index = 0;

        @Override
        public boolean hasNext() {
            return index + 1 <= array.length;
        }

        @Override
        public Integer next() {
            return index++;
        }
    }

    public void add(int i) {
        array[size++] = i;
    }
}

public class UDPClient {
    public static Lock getLock() {
        ReentrantLock reentrantLock = new ReentrantLock();
        reentrantLock.lock();
        return reentrantLock;
    }
    public static void main(String[] args) throws Exception {
        getLock().lock();
    }
}
