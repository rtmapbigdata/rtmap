package cn.rtmap.bigdata.ingest.schedule;

import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import cn.rtmap.bigdata.ingest.base.Extractor;
import cn.rtmap.bigdata.ingest.impl.FTPExtractor;

@DisallowConcurrentExecution
public class FTPSourceJob implements Job {
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobDataMap map = context.getJobDetail().getJobDataMap();
		Context ctx = (Context) map.get("context");
		ChannelProcessor channel = (ChannelProcessor) map.get("channel");

		Extractor ex = new FTPExtractor(channel);
		ex.init(ctx);

		while (ex.prepare()) {
			ex.getData();
			ex.cleanup();
		}
	}
}
