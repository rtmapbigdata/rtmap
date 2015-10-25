package cn.rtmap.flume.source;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Helper to manage configuration parameters and utility methods <p>
 * 
 * Configuration parameters readed from flume configuration file:
 * <tt>type: </tt> cn.rtmap.flume.source.SQLSource <p>
 * <tt>connection.url: </tt> database connection URL <p>
 * <tt>user: </tt> user to connect to database <p>
 * <tt>password: </tt> user password <p>
 * <tt>table: </tt> table to read from <p>
 * <tt>run.query.delay: </tt> delay time to execute each query to database <p>
 * <tt>status.file.path: </tt> Directory to save status file <p>
 * <tt>status.file.name: </tt> Name for status file (saves last row index processed) <p>
 * <tt>batch.size: </tt> Batch size to send events from flume source to flume channel <p>
 * <tt>max.rows: </tt> Max rows to import from DB in one query <p>
 * <tt>custom.query: </tt> Custom query to execute to database (be careful) <p>
 * 
 */

public class SQLSourceHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SQLSourceHelper.class);

    private File file,directory;
    private int runQueryDelay, batchSize, maxRows;
    private String statusFilePath;
    private String statusFileName;
    private String connectionURL;
    private String table;
    private String user;
    private String password;
    private String customQuery;
    private String hibernateDialect;
    private String hibernateDriver;

    private String currentIndex;
    private String maxIndex;
    private String checkColumn;
    private String lastValueQuery;
    private String initIndex;

    private String zkHosts;
    private String znodePath;
    private int zkTimeout;

    private static final String DEFAULT_STATUS_DIRECTORY = "/var/lib/flume";
    private static final int DEFAULT_QUERY_DELAY = 10000;
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int DEFAULT_MAX_ROWS = 10000;
    private static final int DEFAULT_INCREMENTAL_VALUE = 0;

    /**
     * Builds an SQLSourceHelper containing the configuration parameters and
     * usefull utils for SQL Source
     * @param context Flume source context, contains the properties from configuration file
     */
    public SQLSourceHelper(Context context){
        statusFilePath = context.getString("status.file.path", DEFAULT_STATUS_DIRECTORY);
        statusFileName = context.getString("status.file.name");
        connectionURL = context.getString("connection.url");
        table = context.getString("table");
        checkColumn = context.getString("check.column.name");
        runQueryDelay = context.getInteger("run.query.delay",DEFAULT_QUERY_DELAY);
        user = context.getString("user");
        password = context.getString("password");
        directory = new File(statusFilePath);
        customQuery = context.getString("custom.query");
        batchSize = context.getInteger("batch.size",DEFAULT_BATCH_SIZE);
        maxRows = context.getInteger("max.rows",DEFAULT_MAX_ROWS);
        hibernateDialect = context.getString("hibernate.dialect");
        hibernateDriver = context.getString("hibernate.connection.driver_class");
        lastValueQuery = context.getString("last.value.query");
        initIndex = context.getString("check.column.initial.value");

        zkHosts = context.getString("zookeeper.hosts");
        znodePath = context.getString("zookeeper.znode.path");
        zkTimeout = context.getInteger("zookeeper.timeout");

        checkMandatoryProperties();

        if (!(isStatusDirectoryCreated())) {
            createDirectory();
        }

        file = new File(statusFilePath + "/" + statusFileName);
    }

    private String buildQuery() {
        currentIndex = getStatusFileIndex(initIndex);
        String sql = String.format(customQuery, currentIndex, maxIndex);
        LOG.info("custom sql query: {}", sql);
        return sql;
    }

    private boolean isStatusFileCreated(){
        return file.exists() && !file.isDirectory() ? true: false;
    }

    private boolean isStatusDirectoryCreated() {
        return directory.exists() && !directory.isFile() ? true: false;
    }

    /**
     * Converter from a List of Object List to a List of String arrays <p>
     * Useful for csvWriter
     * @param queryResult Query Result from hibernate executeQuery method
     * @return A list of String arrays, ready for csvWriter.writeall method
     */
    public List<String[]> getAllRows(List<List<Object>> queryResult){
        List<String[]> allRows = new ArrayList<String[]>();
        if (queryResult == null || queryResult.isEmpty())
            return allRows;

        String[] row=null;

        for (int i=0; i<queryResult.size();i++)
        {
            List<Object> rawRow = queryResult.get(i);
            row = new String[rawRow.size()];
            for (int j=0; j< rawRow.size(); j++){
                if (rawRow.get(j) != null)
                    row[j] = rawRow.get(j).toString();
                else
                    row[j] = "";
            }
            allRows.add(row);
        }

        return allRows;
    }

    /**
     * Update status file with last read row index    
     */
    public void updateStatusFile(String indexValue) {
        /* Status file creation or update */
        try {
            currentIndex = indexValue;
            Writer writer = new FileWriter(file,false);
            writer.write(connectionURL + '\t');
            writer.write(table + '\t');
            writer.write(currentIndex + "\n");
            writer.close();
        } catch (IOException e) {
            LOG.error("Error writing incremental value to status file!!!",e);
        }
    }

    private String getStatusFileIndex(String defaultStartValue) {
        if (!isStatusFileCreated()) {
            LOG.info("Status file not created, using start value from config file");
            return defaultStartValue;
        }
        else{
            try {
                FileReader reader = new FileReader(file);
                char[] chars = new char[(int) file.length()];
                reader.read(chars);
                String[] statusInfo = new String(chars).split("	");
                if (statusInfo[0].equals(connectionURL) && statusInfo[1].equals(table)) {
                    reader.close();
                    LOG.info(statusFilePath + "/" + statusFileName + " correctly formed");                
                    return statusInfo[2];
                }
                else{
                    LOG.warn(statusFilePath + "/" + statusFileName + " corrupt!!! Deleting it.");
                    reader.close();
                    deleteStatusFile();
                    return defaultStartValue;
                }
            } catch (NumberFormatException | IOException e) {
                LOG.error("Corrupt index value in file!!! Deleting it.", e);
                deleteStatusFile();
                return defaultStartValue;
            }
        }
    }

    private void deleteStatusFile() {
        if (file.delete()){
            LOG.info("Deleted status file: {}",file.getAbsolutePath());
        }else{
            LOG.warn("Error deleting file: {}",file.getAbsolutePath());
        }
    }

    private void checkMandatoryProperties() {
        
        if (statusFileName == null){
            throw new ConfigurationException("status.file.name property not set");
        }
        if (connectionURL == null){
            throw new ConfigurationException("connection.url property not set");
        }
        if (table == null && customQuery == null){
            throw new ConfigurationException("property table or custom query not set");
        }
        if (password == null){
            throw new ConfigurationException("password property not set");
        }
        if (user == null){
            throw new ConfigurationException("user property not set");
        }
    }

    /*
     * @return String connectionURL
     */
    String getConnectionURL() {
        return connectionURL;
    }

    /*
     * @return boolean pathname into directory
     */
    private boolean createDirectory() {
        return directory.mkdir();
    }

    /*
     * @return long incremental value as parameter from this
     */
    String getCurrentIndex() {
        return currentIndex;
    }

    /*
     * @void set incrementValue
     */
    void setCurrentIndex(String newValue) {
        currentIndex = newValue;
    }

    /*
     * @return String user for database
     */
    String getUser() {
        return user;
    }

    /*
     * @return String password for user
     */
    String getPassword() {
        return password;
    }

    /*
     * @return int delay in ms
     */
    int getRunQueryDelay() {
        return runQueryDelay;
    }

    int getBatchSize() {
        return batchSize;
    }

    int getMaxRows() {
        return maxRows;
    }

    String getQuery() {
        return buildQuery();
    }

    Object getIndexColumn() {
        return checkColumn;
    }

    String getIndexQuery() {
        currentIndex = getStatusFileIndex(initIndex);
        String sql = String.format(lastValueQuery, currentIndex);
        LOG.info("last index query: {}", sql);
        return sql;
    }

    String getHibernateDialect() {
        return hibernateDialect;
    }

    String getHibernateDriver() {
        return hibernateDriver;
    }

    String getTableName() {
        return table;
    }

    void setMaxIndex(String value) {
        maxIndex = value;
    }

    String getZKHosts() {
        return zkHosts;
    }

    String getZKNodePath() {
        return znodePath;
    }

    int getZKTimeout() {
        return zkTimeout;
    }
}
