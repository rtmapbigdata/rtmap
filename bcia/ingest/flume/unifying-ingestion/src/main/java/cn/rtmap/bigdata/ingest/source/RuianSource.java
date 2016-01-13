package cn.rtmap.bigdata.ingest.source;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.quartz.JobDataMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.constant.CommonConstants;
import cn.rtmap.bigdata.ingest.constant.RuianConstants;
import cn.rtmap.bigdata.ingest.schedule.CronScheduler;
import cn.rtmap.bigdata.ingest.schedule.RuianGetJob;

/**
 * use for ruian data exchange
 */
public class RuianSource extends AbstractSource implements EventDrivenSource, Configurable {
	static final Logger logger = LoggerFactory.getLogger(RuianSource.class);
	
	Context ctx;
	CronScheduler getScheduler = new CronScheduler();
	CronScheduler sendScheduler = new CronScheduler();
	
	@Override
	public void start() {
		JobDataMap jdmget = new JobDataMap();
		jdmget.put(CommonConstants.PROP_FLUME_CONTEXT,ctx);
		jdmget.put(CommonConstants.PROP_CRON_EXPRESS, ctx.getString(RuianConstants.CONFIG_GET_EXPRESS));
		jdmget.put(CommonConstants.PROP_SCHED_THREAD, ctx.getString(CommonConstants.CONFIG_SCHED_THREAD));
		getScheduler.start(RuianGetJob.class, jdmget);
		logger.info("----- Start ruian get scheduler finish -----");
		
		super.start();
	}

	@Override
	public void stop() {
		getScheduler.stop();
		sendScheduler.stop();
		super.stop();
	}
	
	@Override
	public void configure(Context context) {
		ctx = context;
	}
}
