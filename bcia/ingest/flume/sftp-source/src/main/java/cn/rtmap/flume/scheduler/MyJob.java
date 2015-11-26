package cn.rtmap.flume.scheduler;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyJob implements Job {
	private static final Logger LOG = LoggerFactory.getLogger(MyJob.class);
	private static volatile AtomicBoolean signal = new AtomicBoolean();

	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		LOG.info("scheduler has triggered");
		setSignal(true);
	}

	public synchronized void setSignal(boolean status) {
		signal.set(status);
		LOG.info("set signal to " + status);
	}

	public boolean isTriggered() {
		return signal.get();
	}
}
