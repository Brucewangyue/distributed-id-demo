package com.vn.id.etcd;

import com.vn.distributed.id.base.AbstractIDGenerator;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.PutResponse;
import io.netty.util.internal.StringUtil;

/**
 * 利用etcd的全局唯一自增 revision 特性
 * 由于 etcd 是 ca，性能比 redis incr 慢很多，比 mysql auto incr 要快很多
 * 生成6000 id要20秒
 */
public class EtcdIDGenerator extends AbstractIDGenerator {
    Client client;
    KV kvClient;
    private final static String ID_PREFIX = "/id/";

    public EtcdIDGenerator(String... address) {
        client = Client.builder().endpoints(address).build();
        kvClient = client.getKVClient();
    }

    @Override
    public long generate() {
        return generate("default");
    }

    public long generate(String bizCode) {
        if (StringUtil.isNullOrEmpty(bizCode)) {
            throw new RuntimeException("EtcdIDGenerator error: parameter bizCode required");
        }

        ByteSequence key = ByteSequence.from(ID_PREFIX.concat(bizCode).getBytes());
        ByteSequence value = ByteSequence.from("1".getBytes());

        try {
            PutResponse putResponse = kvClient.put(key, value).get();
            // todo revision 全局唯一，如果是多个业务表都要分布式id，这样取得不到连续的id，并且要公用一个revision的范围
            long revision = putResponse.getHeader().getRevision();
            return revision;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
