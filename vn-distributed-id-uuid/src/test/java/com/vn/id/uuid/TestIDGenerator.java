package com.vn.id.uuid;

import org.junit.Test;

import java.util.concurrent.*;

public class TestIDGenerator {

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    private static final Integer THREADS = 100;

    @Test
    public void test_generator_id() throws InterruptedException {
        ConcurrentMap<String,String> map = new ConcurrentHashMap<>();
        CountDownLatch countDownLatch = new CountDownLatch(THREADS);
        for (int i = 0; i < THREADS; i++) {
            executorService.execute(()->{
                // 阻塞直到所有线程都走到这一步
                countDownLatch.countDown();
                String id = new UUIDGenerator().generate();
                map.put(id,"1");
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(1,TimeUnit.HOURS);
        System.out.println(map);
    }
}
