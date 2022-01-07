package com.vn.id.mysql;

import com.mysql.cj.util.StringUtils;
import com.vn.distributed.id.base.AbstractIDGenerator;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MysqlNumberSegmentIDGenerator extends AbstractIDGenerator {

    DataSource dataSource;

    private long currentId;

    /**
     * todo 每个bizCode需要自定义step
     */
    private long step = 1000;

    private final static String QUERY_SEGMENT_SQL = "select * from segment_id where biz_code = ?";
    private final static String INSERT_SEGMENT_SQL = "insert segment_id (`max_id`,`step`,`biz_code`,`version`) value(?,?,?,?)";
    private final static String APPLY_SEGMENT_SQL = "update segment_id set `max_id` = ?, `version` = `version`+ 1 where `biz_code` = ? and `version` = ?";

    // biz_code - max_id
    private ConcurrentMap<String, Long> maxIdMap = new ConcurrentHashMap<>();

    public MysqlNumberSegmentIDGenerator(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public String generate() {
        return generate("");
    }

    public String generate(String bizCode) {
        if (StringUtils.isNullOrEmpty(bizCode)) {
            throw new RuntimeException("MysqlNumberSegmentIDGenerator 缺少bizCode参数");
        }

        // 先在缓存取
        Long maxId = maxIdMap.get(bizCode);
        if (null == maxId || maxId == currentId) {
            // 数据库申请号段
            applyIdSegment(bizCode);
        }

        return generatorId();
    }

    private synchronized String generatorId() {
        return (++currentId) + "";
    }

    /**
     * 加锁防止重复申请号段
     *
     * @param bizCode
     * @return
     */
    private synchronized void applyIdSegment(String bizCode) {
        Long maxId = maxIdMap.get(bizCode);
        if (null != maxId && maxId > currentId) {
            return;
        }

        try {
            Connection connection = dataSource.getConnection();
            PreparedStatement queryStatement = connection.prepareStatement(QUERY_SEGMENT_SQL);
            queryStatement.setString(1, bizCode);
            ResultSet resultSet = queryStatement.executeQuery();

            Long id = null;
            Long rs_maxId = null;
            Long rs_step = null;
            Long rs_version = null;

            while (resultSet.next()) {
                id = resultSet.getLong("id");
                rs_maxId = resultSet.getLong("max_Id");
                rs_step = resultSet.getLong("step");
                rs_version = resultSet.getLong("version");
            }

            // 如果不存在就创建新号段
            if (null == id) {
                PreparedStatement insertStatement = connection.prepareStatement(INSERT_SEGMENT_SQL);
                insertStatement.setLong(1, this.step);
                insertStatement.setLong(2, this.step);
                insertStatement.setString(3, bizCode);
                insertStatement.setLong(4, 0);
                int inserted = insertStatement.executeUpdate();
                if (inserted <= 0) {
                    // 插入失败
                    throw new RuntimeException("MysqlNumberSegmentIDGenerator 新增号段失败 biz_code=" + bizCode);
                }

                this.currentId = 0;
                this.maxIdMap.put(bizCode, this.step);

                return;
            }

            // 如果存在就更新号段 = 最大id + 步长
            PreparedStatement updateStatement = connection.prepareStatement(APPLY_SEGMENT_SQL);
            long newMaxId = rs_maxId + rs_step;
            updateStatement.setLong(1, newMaxId);
            updateStatement.setString(2, bizCode);
            updateStatement.setLong(3, rs_version);

            int updated = updateStatement.executeUpdate();
            if (updated <= 0) {
                // 更新失败，说明乐观锁版本被修改，递归继续更新
                // synchronized 支持锁重入
                applyIdSegment(bizCode);
            } else {
                this.currentId = rs_maxId;
                this.maxIdMap.put(bizCode, newMaxId);
                System.out.println("最终起始id：" + (currentId + 1) + "号段：" + maxIdMap);
            }
        } catch (SQLException e) {
            throw new RuntimeException("MysqlNumberSegmentIDGenerator 更新号段失败 biz_code=" + bizCode, e);
        }
    }
}
