package cn.rtmap.bigdata.ingest.source;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.quartz.JobDataMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.schedule.CronScheduler;
import cn.rtmap.bigdata.ingest.schedule.DBExtractorJob;

public class DBSource extends AbstractSource implements EventDrivenSource, Configurable {
	static final Logger logger = LoggerFactory.getLogger(DBSource.class);
	
	Context ctx;
	List<CronScheduler> dbSchedulers = new ArrayList<>();
	
	@Override
	public void start() {
		String dbconfs = ctx.getString(DBConfigConstants.CONFIG_DB_CONFS);
		logger.info("Get database config file parameter: "+dbconfs);
		if (StringUtils.isNotBlank(dbconfs)) {
			for (String dbconf : dbconfs.trim().split(",")) {
				if (StringUtils.isNotBlank(dbconf)) {
					InputStream inputStream = null;
					try {
						Properties properties = new Properties();
						inputStream = new FileInputStream(dbconf);
						properties.load(inputStream);
						CronScheduler dbScheduler = new CronScheduler();
						JobDataMap jdm = new JobDataMap();
						jdm.put("context", ctx);
						jdm.put("cron_express",	properties.getProperty(DBConfigConstants.CONFIG_CRON_EXPRESS));
						jdm.put(DBConfigConstants.CONFIG_PROPS, properties);
						dbScheduler.start(DBExtractorJob.class, jdm);
						logger.info("Start database scheduler finish: "+dbconf);
					} catch (IOException e) {
						logger.error("Create database scheduler error: "+dbconf+", "+e.getLocalizedMessage(), e);
					} finally {
						IOUtils.closeQuietly(inputStream);
					}
				}
			}
		}	
		super.start();
	}

	@Override
	public void stop() {
		for(CronScheduler scheduler:dbSchedulers){
			scheduler.stop();
		}
		super.stop();
	}

	@Override
	public void configure(Context context) {
		ctx = context;
	}
}
