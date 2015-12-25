package cn.rtmap.bigdata.ingest.source;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.quartz.JobDataMap;

import cn.rtmap.bigdata.ingest.schedule.CronScheduler;
import cn.rtmap.bigdata.ingest.schedule.FileSourceJob;

public class FileSource extends AbstractSource implements EventDrivenSource, Configurable {
	//static final Logger logger = LoggerFactory.getLogger(FileSource.class);

	//CounterGroup counterGroup = new CounterGroup();
	Context ctx;
	CronScheduler sch;

	@Override
	public void start() {
		JobDataMap dataMap = new JobDataMap();
		String cronExpress = ctx.getString("schedule.express");
		dataMap.put("cron_express", cronExpress);
		//dataMap.put("cron_express", "*/30 * * * * ? *");
		dataMap.put("context", ctx);
		dataMap.put("channel", getChannelProcessor());

		sch = new CronScheduler();
		sch.start(FileSourceJob.class, dataMap);

		super.start();
	}

	@Override
	public void stop() {
		sch.stop();
		super.stop();
	}

	@Override
	public void configure(Context context) {
		ctx = context;
	}
}
