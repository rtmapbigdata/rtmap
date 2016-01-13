package cn.rtmap.bigdata.ingest.schedule;

import org.apache.flume.Context;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import cn.rtmap.bigdata.ingest.base.Extractor;
import cn.rtmap.bigdata.ingest.constant.CommonConstants;
import cn.rtmap.bigdata.ingest.impl.DBExtractor;

/**
 * DB Extractor Job
 */
public class DBExtractorJob implements Job {
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobDataMap map = context.getJobDetail().getJobDataMap();
		Context ctx = (Context) map.get(CommonConstants.PROP_FLUME_CONTEXT);
		//Properties properties=(Properties) map.get(DBConstants.CONFIG_PROPS);
		Extractor extractor = new DBExtractor();
		extractor.init(ctx);
		extractor.getData();
		extractor.cleanup();
	}
	
}
