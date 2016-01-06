package cn.rtmap.flume.source.sftp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;


public class Constants {
    private static final Logger LOGGER = LoggerFactory.getLogger(Constants.class);

    public static final String SFTP_HOSTNAME = getValue("sftp.hostname");
    public static final int SFTP_PORT = getValueAsInt("sftp.port");
    		
    public static final String SFTP_USER = getValue("sftp.user");
    public static final String SFTP_PASSWD = getValue("sftp.passwd");

    public static final String SFTP_ROOT_DIR = getValue("sftp.root.dir");
    public static final String SFTP_ROOT_DIR_BAK = getValue("sftp.root.dir.bak");
    
    public static final String SFTP_FLAG_FILE = getValue("sftp.flag.file");
    public static final String SFTP_FLAG_MD5_FLAG = getValue("sftp.flag.md5.flag");
    
    public static final String SFTP_EXT_NAME = getValue("sftp.ext.name");
    public static final int SFTP_FILE_BLOCK_SIZE = getValueAsInt("sftp.file.block.size");

    /**
     * get value from loader
     *
     * @param key
     * @return
     */
    public static String getValue(String key) {
        return Loader.getValue(key);
    }

    public static int getValueAsInt(String key) {
        return Integer.valueOf(getValue(key));
    }

    /**
     * configuration properties loader
     */
    static class Loader {
        private static Properties properties = new Properties();
        private static final String CONFIG_PATH = "/ftp-config.properties";

        /**
         * load configurations while system start
         */
        static {
            try {
                InputStream inputStream = Constants.class.getResourceAsStream(CONFIG_PATH);
                if (inputStream == null) {
                    LOGGER.error("configuration file path[" + CONFIG_PATH + "] not found, system will exit with code 1.");
                    System.exit(1);
                }
                properties.load(inputStream);
                inputStream.close();
            } catch (IOException e) {
                LOGGER.error("load configuration properties error: " + e.getMessage(), e);
            }
        }

        /**
         * get value from object properties
         *
         * @param key
         * @return
         */
        public static String getValue(String key) {
            return properties.getProperty(key);
        }
    }
}
