package cn.rtmap.bigdata.ingest.schedule;

import java.util.Properties;

import org.apache.flume.Context;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import cn.rtmap.bigdata.ingest.impl.DBExtractor;
import cn.rtmap.bigdata.ingest.source.DBConfigConstants;

/**
 * DB Extractor Job
 */
public class DBExtractorJob implements Job {
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobDataMap map = context.getJobDetail().getJobDataMap();
		Context ctx = (Context) map.get("context");
		Properties properties=(Properties) map.get(DBConfigConstants.CONFIG_PROPS);
		
		DBExtractor extractor = new DBExtractor();
		try {
			extractor.init(ctx, properties);
			extractor.extract();
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}finally{
			extractor.close();
		}
	}
	
	
}
