package com.vn.id.etcd;

import com.vn.distributed.id.base.AbstractIDGenerator;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.PutResponse;
import io.netty.util.internal.StringUtil;

/**
 * 利用etcd的全局唯一自增 revision 特性
 *  由于 etcd 是 ca，性能比 redis incr 慢很多，比 mysql auto incr 要快很多
 *  生成6000 id要20秒
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
    public String generate() {
        return generate("default");
    }

    public String generate(String bizCode) {
        if (StringUtil.isNullOrEmpty(bizCode)) {
            throw new RuntimeException("EtcdIDGenerator error: parameter bizCode required");
        }

        ByteSequence key = ByteSequence.from(ID_PREFIX.concat(bizCode).getBytes());
        ByteSequence value = ByteSequence.from("1".getBytes());

        try {
            PutResponse putResponse = kvClient.put(key, value).get();
            long revision = putResponse.getHeader().getRevision();
            return revision + "";
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}
