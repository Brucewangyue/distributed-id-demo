package com.vn.id.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.PutResponse;
import org.junit.Test;

import java.util.concurrent.*;

public class TestIDGenerator {

    private final static String[] address = {"http://10.0.0.30:2379", "http://10.0.0.31:2379", "http://10.0.0.32:2379"};
    private final static int THREADS = 1;
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private ConcurrentMap<Long, Integer> idsMap = new ConcurrentHashMap<>();

    @Test
    public void test_generate_id() throws InterruptedException {
        long start = System.currentTimeMillis();
        EtcdIDGenerator etcdIDGenerator = new EtcdIDGenerator(address);
        CountDownLatch countDownLatch = new CountDownLatch(THREADS);

        for (int i = 0; i < THREADS; i++) {
            executorService.execute(() -> {
                countDownLatch.countDown();
                long id = etcdIDGenerator.generate("test");
                idsMap.put(id, 1);
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        System.out.println(idsMap.size());
        System.out.println(idsMap);
        System.out.println("耗时：" + (System.currentTimeMillis() - start));
    }

    /**
     * 每个线程一个客户端会直接超时
     * @throws InterruptedException
     */
    @Test
    public void test_etcd_client() throws InterruptedException {
        long start = System.currentTimeMillis();
        CountDownLatch countDownLatch = new CountDownLatch(THREADS);

        for (int i = 0; i < THREADS; i++) {
            executorService.execute(() -> {
                countDownLatch.countDown();
                EtcdIDGenerator etcdIDGenerator = new EtcdIDGenerator(address);
                long id = etcdIDGenerator.generate("test");
                idsMap.put(id, 1);
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        System.out.println(idsMap.size());
        System.out.println(idsMap);
        System.out.println("耗时：" + (System.currentTimeMillis() - start));
    }

    @Test
    public void test_etcd_connect() throws ExecutionException, InterruptedException {
        Client build = Client.builder().endpoints(address).build();
        KV kvClient = build.getKVClient();
        PutResponse putResponse = kvClient.put(ByteSequence.from("key".getBytes()), ByteSequence.from("vale".getBytes())).get();
        System.out.println(putResponse.getHeader().getRevision());
    }
}
