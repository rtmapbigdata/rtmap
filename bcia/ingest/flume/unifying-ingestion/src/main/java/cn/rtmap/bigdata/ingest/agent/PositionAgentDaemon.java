package cn.rtmap.bigdata.ingest.agent;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class PositionAgentDaemon {
    private static final Logger LOG = Logger.getLogger(PositionAgentDaemon.class);

    public static void main(String[] args) {
        PositionDataService service = new PositionDataService();
        Properties pro = new Properties();

        try {
			pro.load(new FileInputStream("/c:/agent/config.properties"));
		} catch (IOException ex) {
			LOG.error("load properties file failed", ex);
			return;
		}

        String[] dirs = pro.getProperty("dirs").split(",");
        String url = pro.getProperty("url");

        String buildid = null;
        String date = null;
        if (args != null && args.length > 0) {
            date = args[0];
        }
        if (args != null && args.length > 1) {
            buildid = args[1];
        }
        for (String bidDir : dirs) {
            LOG.info("################################################################");
            try {
            	LOG.info("lookup config dir : " + bidDir);
            	File f = new File(bidDir);
            	if (f.exists()) {
            		service.sendFiles(bidDir, date, buildid, url);
            	} else {
            		LOG.error("dir not exists: " + bidDir);
            	}
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
