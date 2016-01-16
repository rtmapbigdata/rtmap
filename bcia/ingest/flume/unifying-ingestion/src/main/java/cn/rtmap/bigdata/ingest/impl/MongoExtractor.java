package cn.rtmap.bigdata.ingest.impl;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.base.Extractor;
import cn.rtmap.bigdata.ingest.base.JsonElement;
import cn.rtmap.bigdata.ingest.constant.CommonConstants;
import cn.rtmap.bigdata.ingest.constant.DBConstants;
import cn.rtmap.bigdata.ingest.utils.DateUtils;
import cn.rtmap.bigdata.ingest.utils.IngestUtil;

import com.google.common.collect.ImmutableSet;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

public class MongoExtractor implements Extractor {
	private static final Logger logger = LoggerFactory.getLogger(MongoExtractor.class);
	private String host = null;
	private int port = 27017;
	private String database = null;
	private MongoClient client = null;
	private MongoDatabase db = null;
	private List<String> tables=new ArrayList<String>();
	private String incoming = null;
	private int index = 0;
	private int threshold = 3000;
	
	@Override
	public void init(Context ctx) {
		host=ctx.getString(DBConstants.CONFIG_HOST);
		port=ctx.getInteger(DBConstants.CONFIG_PORT);
		database=ctx.getString(DBConstants.CONFIG_DB);
		incoming=ctx.getString(CommonConstants.CONFIG_INCOMING_DIR);
		client = new MongoClient(host, port);
		db = client.getDatabase(database);
		ImmutableSet<String> keys=ctx.getParameters().keySet();
		for(String key : keys){
			if(key.startsWith(DBConstants.CONFIG_QUERY_KEYPRE)){
				tables.add(key.substring(DBConstants.CONFIG_QUERY_KEYPRE.length()) + ":" + ctx.getString(key));
			}
		}
		index=tables.size();
		logger.info("init mongo source finish with " + tables.size() + " tables: " + host);
	}

	@Override
	public boolean prepare() {
		if(index <= 0){
			return false;
		}
		index--;
		return true;
	}

	@Override
	public Iterator<JsonElement<String, String>> getData() {
		String today = DateUtils.getCurrentDate();
		String yesterday = DateUtils.getDateByCondition(-1);
		//String datetime1 = yesterday + " 00:00:00";
		//String datetime2 = today + " 00:00:00";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date datetime1=null;
		Date datetime2=null;
		try {
			datetime1 = sdf.parse(yesterday + " 00:00:00");
			datetime2 = sdf.parse(today + " 00:00:00");
		} catch (ParseException e1) {
			logger.error(e1.getLocalizedMessage(),e1);
			return null;
		}
		String[] cfgs = tables.get(index).split(":");
		if(cfgs.length != 3){
			logger.error("table cfg error: "+tables.get(index));
			return null;
		}
		String tbName=cfgs[0];
		String whereField=cfgs[1];
		String[] fields=cfgs[2].split(",");
		String fileName = "a_" + yesterday.replaceAll("-", "") +  "_" + tbName.replaceAll("_", "-");
		String filePath = incoming + "/" + yesterday.replaceAll("-", "") + "/day/" + fileName;
		String tmpFile  = filePath + CommonConstants.DEFAULT_TMP_EXTENSION;
		String verfFile = filePath + CommonConstants.DEFAULT_VERF_EXTENSION;
		String zipFile  = filePath + CommonConstants.DEFAULT_ZIP_EXTENSION;
		logger.info("start extract table: " + yesterday + "," + tables.get(index));
		File file=new File(tmpFile);
		if(file.exists()){
			FileUtils.deleteQuietly(file);
			System.out.println(fileName+".tmp file exist, delete finish!");
		}
		MongoCollection<Document> collection = db.getCollection(tbName);
		FindIterable<Document> iterable = collection.find(Filters.and(Filters.gte(whereField,datetime1),Filters.lt(whereField,datetime2)));
		//FindIterable<Document> iterable = collection.find(Filters.lt(whereField,datetime2));//history
		MongoCursor<Document> docs=iterable.iterator();
		List<String> lines=new ArrayList<String>();
		int count = 0;
		try {
			while(docs.hasNext()){
				count++;
				Document document=docs.next();
				lines.add(getLine(document,fields));
				if(lines.size() == threshold){
					FileUtils.writeLines(file, lines, true);
					lines.clear();
				}
			}
			if(lines.size() > 0){
				FileUtils.writeLines(file, lines, true);
				lines.clear();
			}
			if(count > 0){
				IngestUtil.createVerfFile(file, fileName+CommonConstants.DEFAULT_FILE_EXTENSION, verfFile);
				IngestUtil.zipFile(tmpFile,zipFile,true);
				FileUtils.forceDelete(file);
			}
		}catch (Exception e) {
			logger.error(e.getLocalizedMessage(),e);
		}finally{
			logger.info("finish extract,lines total: "+count);
		}
		return null;
	}

	private String getLine(Document document,String[] fields){
		String line="";
		for(String field : fields){
			String value=document.get(field)==null?"":document.get(field).toString();
			if(StringUtils.isNotBlank(line)){
				line+="\t";
			}
			line+=value;
		}
	    return line;
	}
	
	@Override
	public void cleanup() {
		if(client != null){
			client.close();
			logger.info("close client finish!");
		}
	}
}
