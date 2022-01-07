package com.vn.id.mysql;

import com.alibaba.druid.pool.DruidDataSource;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class TestIDGenerator {
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    private static final Integer THREADS = 1000;

    DataSource dataSource;

    @Before
    public void before() throws SQLException {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://10.0.0.193/test");
        dataSource.setUsername("root");
        dataSource.setPassword("123");
        dataSource.setMaxActive(140);
        dataSource.setInitialSize(140);
        dataSource.setMaxWait(40000);
        dataSource.init();
        this.dataSource = dataSource;
    }

    @Test
    public void test_generator_auto_increment_id() throws InterruptedException {
        ConcurrentMap<String, String> map = new ConcurrentHashMap<>();
        CountDownLatch countDownLatch = new CountDownLatch(THREADS);
        MysqlAutoIncrementIDGenerator idGenerator = new MysqlAutoIncrementIDGenerator(dataSource);
        // 1000个线程同时请求时
        // Data source rejected establishment of connection,  message from server: "Too many connections
        for (int i = 0; i < THREADS; i++) {
            executorService.execute(() -> {
                // 阻塞等待所有线程都走到这一步
                countDownLatch.countDown();
                String id = idGenerator.generate();
                map.put(id, "1");
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        System.out.println(map);
    }

    /**
     * 性能比 redis incr 好很多：生成6000个ID快10倍
     * @throws InterruptedException
     */
    @Test
    public void test_generator_number_segment_id() throws InterruptedException {
        long start = System.currentTimeMillis();

        ConcurrentMap<String, String> map = new ConcurrentHashMap<>();
        CountDownLatch countDownLatch = new CountDownLatch(THREADS);
        MysqlNumberSegmentIDGenerator idGenerator = new MysqlNumberSegmentIDGenerator(dataSource);
        for (int i = 0; i < THREADS; i++) {
            executorService.execute(() -> {
                // 阻塞等待所有线程都走到这一步
                countDownLatch.countDown();
                String id = idGenerator.generate("test");
                map.put(id, "1");
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        System.out.println("耗时：" + (System.currentTimeMillis() - start));
        System.out.println(map.size());
        System.out.println(map);
    }

    @Test
    public void test_generator_optimistic_lock() throws InterruptedException {
        long start = System.currentTimeMillis();

        for (int i = 0; i < 5; i++) {
            executorService.execute(() -> {

                ExecutorService newCachedThreadPool = Executors.newCachedThreadPool();
                ConcurrentMap<String, String> map = new ConcurrentHashMap<>();
                CountDownLatch countDownLatch = new CountDownLatch(THREADS);
                MysqlNumberSegmentIDGenerator idGenerator = new MysqlNumberSegmentIDGenerator(dataSource);
                for (int j = 0; j < THREADS; j++) {
                    newCachedThreadPool.execute(() -> {
                        // 阻塞等待所有线程都走到这一步
                        countDownLatch.countDown();
                        String id = idGenerator.generate("test");
                        map.put(id, "1");
                    });
                }

                newCachedThreadPool.shutdown();
                try {
                    newCachedThreadPool.awaitTermination(1, TimeUnit.HOURS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(map.size());
                System.out.println(map);
            });
        }

        TimeUnit.SECONDS.sleep(5);

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        System.out.println("耗时：" + (System.currentTimeMillis() - start));

    }

    /**
     * 线程非同步
     */
    @Test
    public void test_hashmap() throws InterruptedException {
        Map<String, String> map = new HashMap<>();

        CountDownLatch countDownLatch = new CountDownLatch(THREADS);
        for (int i = 0; i < THREADS; i++) {
            String key = i + "";
            executorService.execute(() -> {
                // 阻塞等待所有线程都走到这一步
                countDownLatch.countDown();
                map.put(key, "1");
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        System.out.println(map.size());
    }

    @Test
    public void test_concurrentHashMap() throws InterruptedException {
        ConcurrentMap<String, String> map = new ConcurrentHashMap<>();

        CountDownLatch countDownLatch = new CountDownLatch(THREADS);
        for (int i = 0; i < THREADS; i++) {
            String key = i + "";
            executorService.execute(() -> {
                // 阻塞等待所有线程都走到这一步
                countDownLatch.countDown();
                map.put(key, "1");
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        System.out.println("ConcurrentMap" + map.size());
    }
}
