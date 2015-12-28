package cn.rtmap.bigdata.ingest.agent;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class PositionAgentDaemon {
    private static final Logger LOG = Logger.getLogger(PositionAgentDaemon.class);

    public static void main(String[] args) throws IOException {
        PositionDataService service = new PositionDataService();

        Properties pro = new Properties();
        pro.load(new FileInputStream("/config.properties"));
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
                service.sendFiles(bidDir, date, buildid, url);
            	//service.sendFiles(bidDir, "2015-08-26", buildid, url);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
