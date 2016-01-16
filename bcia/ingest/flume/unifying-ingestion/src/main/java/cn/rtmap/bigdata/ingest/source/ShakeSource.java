package cn.rtmap.bigdata.ingest.source;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.quartz.JobDataMap;

import cn.rtmap.bigdata.ingest.schedule.CronScheduler;
import cn.rtmap.bigdata.ingest.schedule.ShakeSourceJob;

public class ShakeSource extends AbstractSource implements EventDrivenSource, Configurable {
	private Context ctx;
	private CronScheduler sch;

	@Override
	public void configure(Context context) {
		ctx = context;
	}

	@Override
	public void start() {
		JobDataMap dataMap = new JobDataMap();
		String cronExpress = ctx.getString("sched.express");
		String thrdCount = ctx.getString("sched.threads");

		dataMap.put("cron_express", cronExpress);
		dataMap.put("context", ctx);
		dataMap.put("channel", getChannelProcessor());
		dataMap.put("sched.threads", thrdCount);

		sch = new CronScheduler();
		sch.start(ShakeSourceJob.class, dataMap);
		super.start();
	}

	@Override
	public void stop() {
		sch.stop();
		super.stop();
	}
}
