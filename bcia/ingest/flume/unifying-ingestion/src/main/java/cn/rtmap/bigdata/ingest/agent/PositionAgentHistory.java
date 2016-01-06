package cn.rtmap.bigdata.ingest.agent;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.log4j.Logger;

public class PositionAgentHistory {
	private static final Logger LOG = Logger.getLogger(PositionAgentHistory.class);

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
	        String[] urls = pro.getProperty("urls").split(",");

	        for (String bidDir : dirs) {
	            LOG.info("################################################################");
	            try {
	            	LOG.info("lookup config dir : " + bidDir);
	            	File base = new File(bidDir);
	            	if (base.exists()) {
	            		File[] bids = base.listFiles();		            	
		            	for (File bid : bids) {
		            		if (bid.isDirectory() && bid.getName().startsWith("86")) {
		            			for (File dateDir : bid.listFiles()) {
		                    		LOG.info(bidDir + "\t" + dateDir.getName() + "\t" + bid.getName());
		            				service.sendFiles(bidDir, dateDir.getName(), bid.getName(), urls);
		            			}
		            		}
		            	}
	            	} else {
	            		LOG.error("dir not exists: " + bidDir);
	            	}
	            } catch (Exception e) {
	                e.printStackTrace();
	            }
	        }
	    }
}
