package com.vn.id.redis;

import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import org.junit.Test;

import java.util.concurrent.*;

public class TestIDGenerator {
    private ExecutorService executorService = Executors.newCachedThreadPool();

    private static final Integer THREADS = 6000;

    /**
     * 性能比 mysql segment差很多
     * @throws InterruptedException
     */
    @Test
    public void test_generator() throws InterruptedException {
        long start = System.currentTimeMillis();

        ConcurrentMap<Long, Integer> idsMap = new ConcurrentHashMap<>();
        CountDownLatch countDownLatch = new CountDownLatch(THREADS);
        RedisIncrIDGenerator redisIncrIDGenerator = new RedisIncrIDGenerator();

        for (int i = 0; i < THREADS; i++) {
            executorService.execute(() -> {
                countDownLatch.countDown();
                long id = redisIncrIDGenerator.generate();
                idsMap.put(id, 1);
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        System.out.println("耗时：" + (System.currentTimeMillis() - start));
        System.out.println("id数：" + idsMap.size());
    }
}
