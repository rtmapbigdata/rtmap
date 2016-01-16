package cn.rtmap.bigdata.ingest.schedule;

import java.util.Iterator;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import cn.rtmap.bigdata.ingest.base.Extractor;
import cn.rtmap.bigdata.ingest.base.JsonElement;
import cn.rtmap.bigdata.ingest.impl.ShakeExtractor;

@DisallowConcurrentExecution
public class ShakeSourceJob extends BaseJob implements Job {

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobDataMap map = context.getJobDetail().getJobDataMap();
		Context ctx = (Context) map.get("context");
		ChannelProcessor channel = (ChannelProcessor) map.get("channel");
		
		Extractor extractor = new ShakeExtractor();
		extractor.init(ctx);

		if (extractor.prepare()) {
			Iterator<JsonElement<String, String>> it = extractor.getData();
			while (it.hasNext()) {
				Event event = buildEvent(it.next());
				channel.processEvent(event);
			}
			extractor.cleanup();
		}
	}
}
