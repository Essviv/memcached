package com.cmcc.syw;

import com.google.gson.Gson;
import net.spy.memcached.CachedData;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by sunyiwei on 2015/11/19.
 */
public class Main {
    public static void main(String[] args) throws IOException {
        final MemcachedClient client = new MemcachedClient(
                new InetSocketAddress("192.168.32.68", 11211), new InetSocketAddress("192.168.32.68", 11212));

        int count = 10000;
        final Queue<User> users = new ConcurrentLinkedQueue<User>();
        for (int i = 0; i < count; i++) {
            users.add(new User("sunyiwei_" + i, 27));
        }

        Long start = System.currentTimeMillis();

        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executor.execute(new Runnable() {
                public void run() {
                    while (!users.isEmpty()) {
                        User user = users.poll();
                        setOneTime(client, user.getName(), user);
                    }
                }
            });
        }

        executor.shutdown();
        try {
            while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                System.out.println("Waiting...");
                continue;
            }

            System.out.println((double) (System.currentTimeMillis() - start) / 1000 + " secs");
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println(new Gson().toJson(new User("sunyiwei_" + 9, 27)));
        System.out.println(new Gson().toJson(new User("sunyiwei_" + 9, 27)).length());
    }

    private static void setOneTime(final MemcachedClient client, String key, User user) {
        final Transcoder<User> transcoder = new Transcoder<User>() {
            public boolean asyncDecode(CachedData d) {
                return false;
            }

            public CachedData encode(User o) {
                return new CachedData(0, new Gson().toJson(o).getBytes(), getMaxSize());
            }

            public User decode(CachedData d) {
                return new Gson().fromJson(new String(d.getData()), User.class);
            }

            public int getMaxSize() {
                return 10 * 1024 * 1024;
            }
        };

        client.set(key, 0, user, transcoder);
    }
}

