package cn.rtmap.bigdata.ingest.schedule;

import java.util.Iterator;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.DisallowConcurrentExecution;

import cn.rtmap.bigdata.ingest.base.Extractor;
import cn.rtmap.bigdata.ingest.base.JsonElement;
import cn.rtmap.bigdata.ingest.impl.FileExtractor;

@DisallowConcurrentExecution
public class FileSourceJob implements Job {
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobDataMap map = context.getJobDetail().getJobDataMap();
		Context ctx = (Context) map.get("context");
		ChannelProcessor channel = (ChannelProcessor) map.get("channel");
		
		Extractor extractor = new FileExtractor();
		extractor.init(ctx);

		while (extractor.prepare()) {
			Iterator<JsonElement<String, String>> it = extractor.getData();
			while (it.hasNext()) {
				Event event = buildEvent(it.next());
				channel.processEvent(event);
			}
			extractor.cleanup();
		}
	}

	private Event buildEvent(JsonElement<String, String> o) {
		byte[] body = o.getBody();
		Map<String, String> headers = o.getHeaders();

		// format the message
		return EventBuilder.withBody(body, headers);
	}
}
