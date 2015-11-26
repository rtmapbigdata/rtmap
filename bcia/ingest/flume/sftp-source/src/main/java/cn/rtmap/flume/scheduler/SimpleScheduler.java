package cn.rtmap.flume.scheduler;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;

import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.CronScheduleBuilder;
import org.quartz.impl.StdSchedulerFactory;

import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleScheduler {
	private static final Logger LOG = LoggerFactory.getLogger(SimpleScheduler.class);
	private static final String DEFAULT_JOB_GROUP = "DEFAULT_FLUME_JOB_GROUP";
	private static final String DEFAULT_TRIGGER_GROUP = "DEFAULT_FLUME_TRIGGER_GROUP";

	//public static void main(String[] args) {
	//	scheduleJob("*/5 * * * * ? *");
	//}

	public void scheduleJob(String cornExpress, String jobKey, String triggerKey) {
		SchedulerFactory fac = new StdSchedulerFactory();

		try {
			Scheduler sch = fac.getScheduler();
			JobDetail job = JobBuilder.newJob(MyJob.class).withIdentity(jobKey, DEFAULT_JOB_GROUP).build();
			Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey, DEFAULT_TRIGGER_GROUP).withSchedule(CronScheduleBuilder.cronSchedule(cornExpress)).startNow().build();
			sch.scheduleJob(job, trigger);
			sch.start();
		} catch (SchedulerException e) {
			LOG.error("Job schedule error", e);
		}
	}
}
 