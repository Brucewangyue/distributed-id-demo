package com.vn.id.mysql;

import com.vn.distributed.id.base.AbstractIDGenerator;

import javax.sql.DataSource;
import java.sql.*;

/**
 * 自增id，高并发情况下，数据库压力大，DB单点存在宕机风险
 *  性能极差，只用于测试
 *  只能通过外部使用连接池优化
 */
public class MysqlAutoIncrementIDGenerator extends AbstractIDGenerator {

    private final static String SQL = "insert seq_id value()";

    DataSource dataSource;

    public MysqlAutoIncrementIDGenerator(DataSource dataSource) {
        this.dataSource = dataSource;
//        try {
//            Class.forName("com.mysql.cj.jdbc.Driver");
//            con = DriverManager.getConnection("jdbc:mysql://10.9.33.193/filar", "root", "F@20dms2020");
//            // 测试连接是否成功
//            System.out.println(con);
//            statement = con.createStatement();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public long generate() {
        try {
            Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(SQL,Statement.RETURN_GENERATED_KEYS);
            statement.executeUpdate();
            ResultSet generatedKeys = statement.getGeneratedKeys();
            // 必要，移动指针到数据起始位置
            generatedKeys.next();
            long id = generatedKeys.getLong(1);
            return id;
        } catch (SQLException e) {
            throw new RuntimeException("MysqlAutoIncrementIDGenerator 获取id失败", e);
        }
    }
}
