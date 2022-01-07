package com.vn.id.redis;

import com.vn.distributed.id.base.AbstractIDGenerator;
import redis.clients.jedis.JedisPooled;

/**
 * 因为Redis是单线程的，所以天然没有资源争用问题，可以采用 incr 指令，实现ID的原子性自增。
 * 但是因为Redis的数据备份-RDB，会存在漏掉数据的可能，所以理论上存在已使用的ID再次被使用，
 * 所以备份方式可以加上AOF方式，这样的话性能会有所损耗。
 *
 * 性能差，每生成一个Id需要发送一次网络请求，远不及数据库
 *
 * 注意：可能会因为进行RDB的时候会触发客户端读超时，所以超时时间设置久一点。
 */
public class RedisIncrIDGenerator extends AbstractIDGenerator {
    private JedisPooled jedis;
    private final static String IDS_KEY = "IDS";

    public RedisIncrIDGenerator() {
        jedis = new JedisPooled("10.0.0.99", 6379);
    }

    @Override
    public String generate() {
        long newID = jedis.incr(IDS_KEY);
        return newID + "";
    }
}
