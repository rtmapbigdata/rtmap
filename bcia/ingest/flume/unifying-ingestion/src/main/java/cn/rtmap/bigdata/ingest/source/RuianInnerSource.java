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
import cn.rtmap.bigdata.ingest.schedule.RuianInnerJob;

/**
 * use for ruian data exchange
 * cluster inner put hive query data into kafka
 */
public class RuianInnerSource extends AbstractSource implements EventDrivenSource, Configurable {
	static final Logger logger = LoggerFactory.getLogger(RuianInnerSource.class);
	
	Context ctx;
	CronScheduler scheduler = null;
	
	@Override
	public void start() {
		JobDataMap jdm = new JobDataMap();
		jdm.put(CommonConstants.PROP_FLUME_CONTEXT,ctx);
		jdm.put(CommonConstants.PROP_CRON_EXPRESS, ctx.getString(CommonConstants.CONFIG_CRON_EXPRESS));
		jdm.put(CommonConstants.PROP_SCHED_THREAD, ctx.getString(CommonConstants.CONFIG_SCHED_THREAD));
		jdm.put(CommonConstants.PROP_FLUME_CHANNEL, getChannelProcessor());
		scheduler = new CronScheduler();
		scheduler.start(RuianInnerJob.class, jdm);
		logger.info("--------- Start ruian cluster inner scheduler finish ---------");
		super.start();
	}

	@Override
	public void stop() {
		scheduler.stop();
		super.stop();
	}
	
	@Override
	public void configure(Context context) {
		ctx = context;
	}
}
