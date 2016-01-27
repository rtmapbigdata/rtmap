package cn.rtmap.bigdata.ingest.source;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.quartz.JobDataMap;

import cn.rtmap.bigdata.ingest.schedule.CronScheduler;
import cn.rtmap.bigdata.ingest.schedule.FTPSourceJob;

public class FTPSource extends AbstractSource implements EventDrivenSource, Configurable {
	Context ctx;
	CronScheduler sch;

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
		sch.start(FTPSourceJob.class, dataMap);
		super.start();
	}

	@Override
	public void stop() {
		if (sch != null)
			sch.stop();
		super.stop();
	}

	@Override
	public void configure(Context context) {
		ctx = context;
	}
}
