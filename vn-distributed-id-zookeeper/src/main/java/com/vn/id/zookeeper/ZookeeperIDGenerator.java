package com.vn.id.zookeeper;

import com.vn.distributed.id.base.AbstractIDGenerator;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 利用zookeeper中的顺序节点的特性
 */
public class ZookeeperIDGenerator extends AbstractIDGenerator {
    private final static String Address = "10.0.0.99:2181";

    private final static String ROOT = "/ids";
    private final static String ID_PREFIX_NAME = "/id_";

    private CuratorFramework zkClient;

    public ZookeeperIDGenerator() {
        zkClient = CuratorFrameworkFactory.builder()
                .connectString(Address)
                .connectionTimeoutMs(5000)
                .sessionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        zkClient.start();

        try {
            Stat stat = zkClient.checkExists().forPath(ROOT);
            if (null == stat) {
                zkClient.create().withMode(CreateMode.PERSISTENT).forPath(ROOT);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String generate() {
        return null;
    }

    public String generate(String bizPath) {
        checkNodeExist(bizPath);

        String id = null;
        try {
            String idPath = zkClient.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(ROOT.concat(bizPath).concat(ID_PREFIX_NAME));
            System.out.println("idPath:" + idPath);

            // 异步删除节点
            ExecutorService executorService = Executors.newFixedThreadPool(10);
            executorService.execute(() -> {
                try {
                    zkClient.delete().forPath(idPath);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            // 解析id
            id = splitId(idPath);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return id;
    }

    private String splitId(String idPath) {
        int i = idPath.lastIndexOf(ID_PREFIX_NAME);
        if (i >= 0) {
            i += ID_PREFIX_NAME.length();
            return idPath.substring(i);
        }

        throw new RuntimeException("ZookeeperIDGenerator id格式不符合规范");
    }

    private void checkNodeExist(String bizPath) {
        try {
            // todo 加本地缓存优化
            String bizIdFullPath = ROOT.concat(bizPath);
            Stat stat = zkClient.checkExists().forPath(bizIdFullPath);
            if (null == stat) {
                zkClient.create().withMode(CreateMode.PERSISTENT).forPath(bizIdFullPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
