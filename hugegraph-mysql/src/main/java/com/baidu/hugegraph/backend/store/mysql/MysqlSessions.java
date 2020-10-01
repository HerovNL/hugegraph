/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.backend.store.mysql;

import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendSession.AbstractBackendSession;
import com.baidu.hugegraph.backend.store.BackendSessionPool;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;

public class MysqlSessions extends BackendSessionPool {

    private static final Logger LOG = Log.logger(MysqlStore.class);

    private HugeConfig config;
    private String database;
    private volatile boolean opened;

    public MysqlSessions(HugeConfig config, String database, String store) {
        super(config, database + "/" + store);
        this.config = config;
        this.database = database;
        this.opened = false;
    }

    @Override
    public HugeConfig config() {
        return this.config;
    }

    public String database() {
        return this.database;
    }

    public String escapedDatabase() {
        return MysqlUtil.escapeString(this.database());
    }

    /**
     * Try connect with specified database, will not reconnect if failed
     *
     * @throws SQLException if a database access error occurs
     */
    @Override
    public synchronized void open() throws Exception {
        try (Connection conn = getIndividualConnection()) {
            this.opened = true;
        }
    }

    @Override
    protected boolean opened() {
        return this.opened;
    }

    @Override
    protected void doClose() {
        // pass
    }

    @Override
    public Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected Session newSession() {
        return new Session();
    }

    public void createDatabase() {
        // Create database with non-database-session
        LOG.debug("Create database: {}", this.database());

        String sql = this.buildCreateDatabase(this.database());
        try (Connection conn = getIndividualConnection()) {
            conn.createStatement().execute(sql);
        } catch (SQLException e) {
            if (!e.getMessage().endsWith("already exists")) {
                throw new BackendException("Failed to create database '%s'", e,
                        this.database());
            }
            // Ignore exception if database already exists
        }
    }

    public void dropDatabase() {
        LOG.debug("Drop database: {}", this.database());

        String sql = this.buildDropDatabase(this.database());
        try (Connection conn = getIndividualConnection()) {
            conn.createStatement().execute(sql);
        } catch (SQLException e) {
            if (e.getCause() instanceof SocketTimeoutException) {
                LOG.warn("Drop database '{}' timeout", this.database());
            } else {
                throw new BackendException("Failed to drop database '%s'", e,
                        this.database());
            }
        }
    }

    public boolean existsDatabase() {
        try (Connection conn = getIndividualConnection();
             ResultSet result = conn.getMetaData().getCatalogs()) {
            while (result.next()) {
                String dbName = result.getString(1);
                if (dbName.equals(this.database())) {
                    return true;
                }
            }
        } catch (Exception e) {
            throw new BackendException("Failed to obtain database info", e);
        }
        return false;
    }

    public boolean existsTable(String table) {
        String sql = this.buildExistsTable(table);
        try (Connection conn = getIndividualConnection();
             ResultSet result = conn.createStatement().executeQuery(sql)) {
            return result.next();
        } catch (Exception e) {
            throw new BackendException("Failed to obtain table info", e);
        }
    }

    public void resetConnections() {
        // Close the under layer connections owned by each thread
        this.forceResetSessions();
    }

    protected String buildCreateDatabase(String database) {
        return String.format("CREATE DATABASE IF NOT EXISTS %s " +
                        "DEFAULT CHARSET utf8 COLLATE utf8_bin;",
                database);
    }

    protected String buildDropDatabase(String database) {
        return String.format("DROP DATABASE IF EXISTS %s;", database);
    }

    protected String buildExistsTable(String table) {
        return String.format("SELECT * FROM information_schema.tables " +
                        "WHERE table_schema = '%s' " +
                        "AND table_name = '%s' LIMIT 1;",
                this.escapedDatabase(),
                MysqlUtil.escapeString(table));
    }

    private Connection getIndividualConnection() throws SQLException {
        return MysqlDataSource.getConnection(config);
    }

    private Connection getConnection4Thread() throws SQLException {
        return MysqlDataSource.getConnection4Thread(config);
    }

    private Connection getConnection4Thread(boolean autoCommit) throws SQLException {
        Connection temp = getConnection4Thread();
        if (temp.getAutoCommit() != autoCommit) {
            temp.setAutoCommit(autoCommit);
        }
        return temp;
    }

    private void releaseConnection4Thread() {
        MysqlDataSource.releaseConnection4Thread();
    }


    public class Session extends AbstractBackendSession {
        private Map<String, PreparedStatement> statements;
        private int count;

        public Session() {
            this.statements = new HashMap<>();
            this.count = 0;
        }

        public HugeConfig config() {
            return MysqlSessions.this.config();
        }

        public String database(){
            return MysqlSessions.this.database;
        }

        @Override
        public void open() {
            try {
                getConnection4Thread();
                opened = true;
            } catch (SQLException e) {
                opened = false;
                throw new BackendException("Failed to open connection", e);
            }
        }

        @Override
        public void close() {
            clearStatements();
            releaseConnection4Thread();
        }

        private boolean clearStatements(){
            boolean success = true;
            for (PreparedStatement statement : this.statements.values()) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    success = false;
                    LOG.error("Failed to close statements",e);
                }
            }
            this.statements.clear();
            return success;
        }


        @Override
        public boolean opened() {
            return this.opened;
        }

        @Override
        public boolean closed() {
            return !this.opened;
        }

        public void clear() {
            this.count = 0;
            if (!clearStatements()) {
                /*
                 * Will throw exception when the database connection error,
                 * we clear statements because clearBatch() failed
                 */
                this.statements = new HashMap<>();
            }
        }

        public void begin() throws SQLException {
            getConnection4Thread(false);
        }

        public void endAndLog() {
            try {
                getConnection4Thread(true);
            } catch (SQLException e) {
                LOG.warn("Failed to set connection to auto-commit status", e);
            }
        }

        @Override
        public Integer commit() {
            int updated = 0;
            try {
                for (PreparedStatement statement : this.statements.values()) {
                    updated += IntStream.of(statement.executeBatch()).sum();
                }
                getConnection4Thread().commit();
                this.clear();
            } catch (SQLException e) {
                throw new BackendException("Failed to commit", e);
            }
            /*
             * Can't call endAndLog() in `finally` block here.
             * Because If commit already failed with an exception,
             * then rollback() should be called. Besides rollback() can only
             * be called when autocommit=false and rollback() will always set
             * autocommit=true. Therefore only commit successfully should set
             * autocommit=true here
             */
            this.endAndLog();
            return updated;
        }

        @Override
        public void rollback() {
            this.clear();
            try {
                getConnection4Thread().rollback();
            } catch (SQLException e) {
                throw new BackendException("Failed to rollback", e);
            } finally {
                this.endAndLog();
            }
        }

        @Override
        public boolean hasChanges() {
            return this.count > 0;
        }

        @Override
        public void reconnectIfNeeded() {
        }

        @Override
        public void reset() {
            clearStatements();
        }

        public ResultSet select(String sql) throws SQLException {
            return getConnection4Thread().createStatement().executeQuery(sql);
        }

        public boolean execute(String sql) throws SQLException {
            return getConnection4Thread(true).createStatement().execute(sql);
        }

        public void add(PreparedStatement statement) {
            try {
                // Add a row to statement
                statement.addBatch();
                this.count++;
            } catch (SQLException e) {
                throw new BackendException("Failed to add statement '%s' " +
                        "to batch", e, statement);
            }
        }

        public PreparedStatement prepareStatement(String sqlTemplate)
                throws SQLException {
            PreparedStatement statement = this.statements.get(sqlTemplate);
            if (statement == null) {
                statement = getConnection4Thread().prepareStatement(sqlTemplate);
                this.statements.putIfAbsent(sqlTemplate, statement);
            }
            return statement;
        }
    }
}
