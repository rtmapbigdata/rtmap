package cn.rtmap.bigdata.ingest.mongo;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.bson.Document;

import cn.rtmap.bigdata.ingest.source.DBConfigConstants;
import cn.rtmap.bigdata.ingest.source.FileSourceConfigurationConstants;
import cn.rtmap.bigdata.ingest.utils.DateUtils;
import cn.rtmap.bigdata.ingest.utils.SQLUtil;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/**
 * export to file from market mongodb
 */
public class MongoDataBackSR {
	public static String PATH = "/home/rtmap/temp/mongo";
	public static String host = "10.44.166.121";
	public static String database = "promo";
	public static int port = 27017;
	public static int threshold = 5000;
	public static boolean debugger = false;
	
	public static void main(String[] args) {
		System.out.println("***************** start *****************");
		MongoClient client = null;
		try {
			if(args.length > 0){
				threshold=5;
				debugger=true;
			}
			client = new MongoClient(host, port);
			MongoDatabase db = client.getDatabase(database);
			extract(db);
			zip();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(client != null){
				client.close();
			}
			System.out.println("***************** finish *****************");
		}
	}
	
	public static void extract(MongoDatabase db) throws Exception{
		String table="shake_record";
		System.out.println(">>>>> start table shake_record");
		MongoCollection<Document> collection = db.getCollection(table);
		FindIterable<Document> iterable = null;
		if(debugger){
			iterable=collection.find().limit(200);
		}else{
			iterable=collection.find();
		}
		MongoCursor<Document> docs=iterable.iterator();
		Map<String, List<String>> datas = new HashMap<String, List<String>>();
		String line = null;
		while(docs.hasNext()){
			Document document=docs.next();
			String create_date=document.getDate("create_time")==null?"":DateUtils.formatDate(document.getDate("create_time"), "yyyy-MM-dd");
			line=shake_record(document);
			if(!datas.containsKey(create_date)){
				datas.put(create_date, new ArrayList<String>());
			}
			datas.get(create_date).add(line);
	    	if(datas.get(create_date).size() >= threshold){
	    		String fileName = "a_" + create_date.replaceAll("-", "") +  "_" + table.replaceAll("_", "-");
	    		String filePath = PATH + "/" + create_date.replaceAll("-", "") + "/day/" + fileName;
	    		String datFile = filePath + DBConfigConstants.CONFIG_FILE_EXTENSION;
	    		File file=new File(datFile);
	    		FileUtils.writeLines(file, datas.get(create_date), true);
	    		datas.get(create_date).clear();
	    		System.out.println("write:"+fileName);
	    	}
		}
		for(String key:datas.keySet()){
			if(datas.get(key).size() > 0){
				String fileName = "a_" + key.replaceAll("-", "") +  "_" + table.replaceAll("_", "-");
	    		String filePath = PATH + "/" + key.replaceAll("-", "") + "/day/" + fileName;
	    		String datFile = filePath + DBConfigConstants.CONFIG_FILE_EXTENSION;
	    		File file=new File(datFile);
	    		FileUtils.writeLines(file, datas.get(key), true);
	    	}
		}
	}
	
	public static void zip(){
		System.out.println("------------------ start zip --------------------");
		File path=new File(PATH);
		File[] dates=path.listFiles();
		for(File datePath:dates){
			File[] days=datePath.listFiles();
			for(File day:days){
				File[] files=day.listFiles();
				for(File file:files){
					String verfFile = file.getAbsolutePath().replaceAll(
							DBConfigConstants.CONFIG_FILE_EXTENSION, FileSourceConfigurationConstants.DEFAULT_VERF_EXTENSION);
					String zipFile = file.getAbsolutePath().replaceAll(
							DBConfigConstants.CONFIG_FILE_EXTENSION, DBConfigConstants.CONFIG_ZIP_EXTENSION);
					System.out.println(zipFile);
					try {
						SQLUtil.createVerfFile(file, file.getName(), verfFile);
						SQLUtil.zipFile(file,zipFile);
						FileUtils.forceDelete(file);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	
	public static String shake_record(Document document) throws Exception{
		//select id,activity_id,start_time,end_time,code,qr_code,ticket,open_id,uuid,major,minor,page_id,create_time from shake_record 
		//where create_time>='rpfield_start_date' and create_time<'rpfield_end_date'
		String id=document.get("id")==null?"":document.get("id").toString();
	    String activity_id=document.get("activity_id")==null?"":document.get("activity_id").toString();
	    String start_time=document.get("start_time")==null?"":document.get("start_time").toString();
	    String end_time=document.get("end_time")==null?"":document.get("end_time").toString();
	    String code=document.get("code")==null?"":document.get("code").toString();
	    String qr_code=document.get("qr_code")==null?"":document.get("qr_code").toString();
	    String ticket=document.get("ticket")==null?"":document.get("ticket").toString();
	    String open_id=document.get("open_id")==null?"":document.get("open_id").toString();
	    String uuid=document.get("uuid")==null?"":document.get("uuid").toString();
	    String major=document.get("major")==null?"":document.get("major").toString();
	    String minor=document.get("minor")==null?"":document.get("minor").toString();
	    String page_id=document.get("page_id")==null?"":document.get("page_id").toString();
	    String create_time=document.getDate("create_time")==null?"":DateUtils.formatDate(document.getDate("create_time"), null);
	    String line=id+"\t"+activity_id+"\t"+start_time+"\t"+end_time+"\t"+code+"\t"+qr_code+"\t"+ticket+"\t"+open_id+"\t"+uuid
	    		+"\t"+major+"\t"+minor+"\t"+page_id+"\t"+create_time;
	    return line;
	}
	
}
