package cn.rtmap.flume.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class OracleConnector {
    public static Connection getConnector() throws ClassNotFoundException, SQLException {
        return getConnector(Constants.AJ_JDBC_URL, Constants.AJ_JDBC_USERNAME, Constants.AJ_JDBC_PASSWORD);
    }

    public static Connection getConnector(String url, String username, String password) throws ClassNotFoundException, SQLException {
        Class.forName(Constants.ORACLE_JDBC_DRIVER);
        return DriverManager.getConnection(url, username, password);
    }
}
