package cn.rtmap.bigdata.ingest.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLOperator.class);

    private Connection connection = null;
    private Statement statement = null;

    private String dirver;
    private String url;
    private String username;
    private String password;
    
    public SQLOperator(String dirver, String url, String username, String password) {
    	this.dirver = dirver; 
    	this.url = url;
    	this.username = username;
    	this.password = password;
    }
    
    public ResultSet executeQuery(String sql) throws SQLException, ClassNotFoundException {
        if (connection == null || connection.isClosed()) {
            connection = SQLConnector.getConnector(dirver, url, username, password);
        }
        if (statement == null || statement.isClosed()) {
            statement = connection.createStatement();
        }
        return statement.executeQuery(sql);
    }

    public int executeUpdate(String sql) throws SQLException, ClassNotFoundException {
        if (connection == null || connection.isClosed()) {
            connection = SQLConnector.getConnector(dirver, url, username, password);
        }
        if (statement == null || statement.isClosed()) {
            statement = connection.createStatement();
        }

        return statement.executeUpdate(sql);
    }

    public void close() {
        try {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOGGER.error("close connection error: " + e.getMessage(), e);
        }
    }
}
