package cn.rtmap.bigdata.ingest.schedule;

import java.util.Properties;
import java.util.Random;

import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CronScheduler {
	static final Logger logger = LoggerFactory.getLogger(CronScheduler.class);
	static final int DEFAULT_THREAD_COUNT = 1;
	
	Random random = new Random(this.hashCode());
	StdSchedulerFactory fac;
	Scheduler sch;
	Properties props;

	public void start(Class <? extends Job> jobClass, JobDataMap dataMap) {
		fac = new StdSchedulerFactory();
		props = new Properties();
		String cexp = dataMap.getString("cron_express");

		String threadCount = dataMap.getString("sched.threads");
		if (threadCount == null) {
			threadCount = String.valueOf(DEFAULT_THREAD_COUNT);
		}

		try {
			props.put("org.quartz.scheduler.instanceName", String.format("sched_%s", Integer.toHexString(random.nextInt())));
			props.put("org.quartz.threadPool.threadCount", threadCount);
			fac.initialize(props);

			sch = fac.getScheduler();
			String jobId = String.format("Job_%s", Integer.toHexString( random.nextInt() ));
			String triggerId = String.format("Trigger_%s", Integer.toHexString( random.nextInt() ));

			JobDetail job = JobBuilder.newJob(jobClass).withIdentity(jobId, org.quartz.Scheduler.DEFAULT_GROUP).usingJobData(dataMap).build();
			Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerId, org.quartz.Scheduler.DEFAULT_GROUP).withSchedule(CronScheduleBuilder.cronSchedule(cexp)).startNow().build();

			sch.scheduleJob(job, trigger);
			sch.start();
		} catch (SchedulerException e) {
			logger.error("Job scheduler error", e);
		}
	}

	public void stop() {
		try {
			sch.shutdown();
		} catch (SchedulerException e) {
			logger.error("Shutdown scheduler error", e);
		}
	}
}
