package cn.rtmap.bigdata.ingest.schedule;

import java.util.Random;

import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CronScheduler {
	static final Logger logger = LoggerFactory.getLogger(CronScheduler.class);

	Random random = new Random(this.hashCode());
	SchedulerFactory fac;
	Scheduler sch;

	public void start(Class <? extends Job> jobClass, JobDataMap dataMap) {
		fac = new StdSchedulerFactory();
		String cexp = dataMap.getString("cron_express");

		try {
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
