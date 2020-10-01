package com.baidu.hugegraph.backend.store.mysql;

import com.alibaba.druid.pool.DruidDataSource;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendUrl;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;
import org.slf4j.Logger;

import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

public class MysqlDataSource {
    private static final Logger LOG = Log.logger(MysqlDataSource.class);

    private static final ConcurrentHashMap<String, DruidDataSource> MAP = new ConcurrentHashMap<>();
    private static final ThreadLocal<Connection> LOCAL = new ThreadLocal<>();

    public static DruidDataSource getDataSource(HugeConfig config) {
        String url = config.get(MysqlOptions.JDBC_URL);
        String user = config.get(MysqlOptions.JDBC_USERNAME);
        String urlAndUser = url + "?" + user;
        return MAP.computeIfAbsent(urlAndUser, key -> create(config));
    }

    private static DruidDataSource create(HugeConfig config) {
        String driver = config.get(MysqlOptions.JDBC_DRIVER);
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new BackendException("Failed to init driver for JDBC");
        }
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(driver);
        try {
            BackendUrl jdbcUrl = new BackendUrl(config.get(MysqlOptions.JDBC_URL), 3306);
            jdbcUrl.ensureHasParameter("useSSL", config.get(MysqlOptions.JDBC_SSL_MODE));
            jdbcUrl.ensureHasParameter("characterEncoding", "UTF-8");
            jdbcUrl.ensureHasParameter("autoReconnect", true);
            jdbcUrl.ensureHasParameter("rewriteBatchedStatements", true);
            jdbcUrl.ensureHasParameter("cachePrepStmts", true);
            jdbcUrl.ensureHasParameter("useServerPrepStmts", true);
            dataSource.setUrl(jdbcUrl.toString());
        } catch (MalformedURLException e) {
            throw new BackendException("Failed to parse JDBC URL from configuration");
        }
        dataSource.setUsername(config.get(MysqlOptions.JDBC_USERNAME));
        dataSource.setPassword(config.get(MysqlOptions.JDBC_PASSWORD));
        dataSource.setMaxActive(config.get(MysqlOptions.JDBC_MAX_ACTIVE));
        dataSource.setMinIdle(1);
        dataSource.setTestWhileIdle(true);
        dataSource.setKeepAlive(true);
        String testSql = config.get(MysqlOptions.JDBC_TEST_SQL);
        dataSource.setValidationQuery(testSql);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(testSql)) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (resultSet.next()) {
                    return dataSource;
                }
                throw new BackendException("Failed to test connection by JDBC");
            }
        } catch (SQLException e) {
            throw new BackendException("Failed to test connection by JDBC");
        }
    }

    /**
     * Get individual connection independent of thread
     *
     * @param config configuration of graph
     * @return individual Connection for graph
     * @throws SQLException if failed
     */
    public static Connection getConnection(HugeConfig config) throws SQLException {
        DruidDataSource dataSource = getDataSource(config);
        return dataSource.getConnection();
    }

    /**
     * Get connection for current thread
     *
     * @param config configuration of graph
     * @return thread local connection for graph
     * @throws SQLException if failed
     */
    public static Connection getConnection4Thread(HugeConfig config) throws SQLException {
        DruidDataSource dataSource = getDataSource(config);
        Connection temp = LOCAL.get();
        if (temp == null || temp.isClosed()) {
            temp = dataSource.getConnection();
            LOCAL.set(temp);
        }
        return temp;
    }

    /**
     * Release connection for current thread
     */
    public static void releaseConnection4Thread() {
        try (Connection connection = LOCAL.get()) {
            if (connection != null) {
                LOCAL.remove();
            }
        } catch (SQLException e) {
            LOG.error("Error occurred when release connection for thread");
        }
    }
}
