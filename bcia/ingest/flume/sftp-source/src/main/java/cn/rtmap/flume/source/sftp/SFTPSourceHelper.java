package cn.rtmap.flume.source.sftp;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SFTPSourceHelper {
	private static final Logger LOG = LoggerFactory.getLogger(SFTPSourceHelper.class);
	private Context context;

	public String getCornScheduleExpress() {
			return context.getString("job.schedule.corn.express");
	}

	public String getCornScheduleJobKey() {
		return context.getString("job.schedule.job.key");
	}

	public String getCornScheduleTriggerKey() {
		return context.getString("job.schedule.trigger.key");
	}

	public SFTPSourceHelper(Context context) {
		this.context = context;
	}
}
