package cn.rtmap.bigdata.ingest.schedule;

import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.base.Extractor;
import cn.rtmap.bigdata.ingest.base.JsonElement;
import cn.rtmap.bigdata.ingest.constant.CommonConstants;
import cn.rtmap.bigdata.ingest.impl.RuianHiveExtractor;
import cn.rtmap.bigdata.ingest.utils.MailSender;

/**
 * use for ruian data exchange
 * cluster inner put hive query data into kafka
 */
public class RuianInnerJob extends BaseJob implements Job {
	private static final Logger logger = LoggerFactory.getLogger(RuianInnerJob.class);
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobDataMap map = context.getJobDetail().getJobDataMap();
		Context ctx = (Context) map.get(CommonConstants.PROP_FLUME_CONTEXT);
		String toAddr=ctx.getString(CommonConstants.CONFIG_ALTER_EMAIL);
		ChannelProcessor channel = (ChannelProcessor) map.get(CommonConstants.PROP_FLUME_CHANNEL);
		
		Extractor extractor = new RuianHiveExtractor();
		extractor.init(ctx);
		int events=0;
		while (extractor.prepare()) {
			Iterator<JsonElement<String, String>> it = extractor.getData();
			while (it.hasNext()) {
				Event event = buildEvent(it.next());
				channel.processEvent(event);
				events++;
			}
		}
		extractor.cleanup();
		if (StringUtils.isNotBlank(toAddr)) {
			String title = "ruian exchange to kafka "+(events==0?"fail":"success");
			String content = "ruian exchange<br>hive query result to kafka finish,event count is "+events;
			MailSender.sendHtmlMail(toAddr.trim(), title, content);
			logger.info("send email to admin: "+ toAddr);
		}else{
			logger.info("send email to admin cancel,email address is not set!");
		}
		logger.info("---------- finish job ----------");
	}
}
