package cn.rtmap.bigdata.ingest.schedule;

import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.base.Extractor;
import cn.rtmap.bigdata.ingest.base.JsonElement;
import cn.rtmap.bigdata.ingest.constant.CommonConstants;
import cn.rtmap.bigdata.ingest.impl.RuianGetExtractor;
import cn.rtmap.bigdata.ingest.utils.MailSender;

/**
 * DB Extractor Job
 */
public class RuianGetJob implements Job {
	private static final Logger logger = LoggerFactory.getLogger(RuianGetJob.class);
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobDataMap map = context.getJobDetail().getJobDataMap();
		Context ctx = (Context) map.get(CommonConstants.PROP_FLUME_CONTEXT);
		String toAddr=ctx.getString(CommonConstants.CONFIG_ALTER_EMAIL);
		Extractor extractor = new RuianGetExtractor();
		extractor.init(ctx);
		Iterator<JsonElement<String, String>> it = extractor.getData();
		if (StringUtils.isNotBlank(toAddr)) {
			logger.info("send email to admin: "+ toAddr);
			JsonElement<String, String> je=it.next();
			String title = (String) je.getHeader(CommonConstants.PROP_MAIL_SUBJECT);
			String content = (String) je.getHeader(CommonConstants.PROP_MAIL_CONTENT);
			MailSender.sendHtmlMail(toAddr.trim(), title, content);
		}
		extractor.cleanup();
	}
	
	
}
