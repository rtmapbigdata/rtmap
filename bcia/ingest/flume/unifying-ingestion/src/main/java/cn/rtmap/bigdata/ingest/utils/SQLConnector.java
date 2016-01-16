package cn.rtmap.bigdata.ingest.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


public class SQLConnector {
    public static Connection getConnector(String dirver, String url, String username, String password) throws ClassNotFoundException, SQLException {
        Class.forName(dirver);
        Properties props = new Properties();

        props.put("user", username);
        props.put("password", password);
        return DriverManager.getConnection(url, props);
    }
}
