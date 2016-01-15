package cn.rtmap.bigdata.ingest.source;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.quartz.JobDataMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.constant.CommonConstants;
import cn.rtmap.bigdata.ingest.schedule.CronScheduler;
import cn.rtmap.bigdata.ingest.schedule.MongoSourceJob;

public class MongoSource extends AbstractSource implements EventDrivenSource, Configurable {
	private static final Logger logger = LoggerFactory.getLogger(FileSource.class);
	private Context ctx = null;
	private CronScheduler sch = null;

	@Override
	public void start() {
		JobDataMap dataMap = new JobDataMap();
		dataMap.put(CommonConstants.PROP_CRON_EXPRESS, ctx.getString(CommonConstants.CONFIG_CRON_EXPRESS));
		dataMap.put(CommonConstants.PROP_FLUME_CONTEXT, ctx);
		sch = new CronScheduler();
		sch.start(MongoSourceJob.class, dataMap);
		logger.info("mongo source scheduler started!");
		super.start();
	}
	
	@Override
	public void stop() {
		sch.stop();
		logger.info("mongo source scheduler stopped!");
		super.stop();
	}
	
	@Override
	public void configure(Context context) {
		ctx = context;
	}
}
