package cn.rtmap.bigdata.ingest.mongo;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.bson.Document;

import cn.rtmap.bigdata.ingest.constant.CommonConstants;
import cn.rtmap.bigdata.ingest.utils.DateUtils;
import cn.rtmap.bigdata.ingest.utils.SQLUtil;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

/**
 * export to file from market mongodb
 */
public class MongoDataBack {
	public static String PATH = "/home/rtmap/temp/mongo";
	public static String host = "";
	public static String database = "promo";
	public static int port = 27017;
	public static int threshold = 10000;
	
	public static void main(String[] args) {
		if(args.length == 0){
			System.out.println("Need Parameter: Date");
			return;
		}
		String date=args[0];
		System.out.println("***************** start:"+date+" *****************");
		MongoClient client = null;
		try {
			PATH = PATH + "/" + date.replaceAll("-", "") + "/day/";
			client = new MongoClient(host, port);
			MongoDatabase db = client.getDatabase(database);
			extract(db,"shake_prizebag_click_log","create_time",date);
			extract(db,"shake_prizebag_exposure_log","create_time",date);
			extract(db,"shake_record","create_time",date);
			extract(db,"shake_prize_close_log","create_time",date);
			extract(db,"shake_effective_exposure_log","create_time",date);
			extract(db,"shake_query_prize_log","create_time",date);
			extract(db,"shake_prize_exposure_log","create_time",date);
			extract(db,"shake_operation_log","create_time",date);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(client != null){
				client.close();
			}
			System.out.println("***************** finish *****************");
		}
	}
	
	public static void extract(MongoDatabase db, String table, String field, String date) throws Exception{
		String start=date+" 00:00:00";
		String end=DateUtils.getDateByCondition(1, date)+" 00:00:00";
		String fileName = "a_" + date.replaceAll("-", "") +  "_" + table.replaceAll("_", "-");
		String filePath = PATH + fileName;
		String datFile = filePath + CommonConstants.DEFAULT_FILE_EXTENSION;
		String verfFile = filePath + CommonConstants.DEFAULT_VERF_EXTENSION;
		System.out.println(">>>>> start table: "+table);
		File file=new File(datFile);
		if(file.exists()){
			FileUtils.deleteQuietly(file);
			System.out.println("file exist,delete finish!");
			file=null;
			file=new File(datFile);
		}
		MongoCollection<Document> collection = db.getCollection(table);
		FindIterable<Document> iterable = collection.find(Filters.and(Filters.gte(field,start),Filters.lt(field,end)));
		MongoCursor<Document> docs=iterable.iterator();
		List<String> lines=new ArrayList<String>();
		int count = 0;
		while(docs.hasNext()){
			count++;
			Document document=docs.next();
			if("shake_prizebag_click_log".equals(table)){
				lines.add(shake_prizebag_click_log(document));
			}else if("shake_prizebag_exposure_log".equals(table)){
				lines.add(shake_prizebag_exposure_log(document));
			}else if("shake_record".equals(table)){
				lines.add(shake_record(document));
			}else if("shake_prize_close_log".equals(table)){
				lines.add(shake_prize_close_log(document));
			}else if("shake_effective_exposure_log".equals(table)){
				lines.add(shake_effective_exposure_log(document));
			}else if("shake_query_prize_log".equals(table)){
				lines.add(shake_query_prize_log(document));
			}else if("shake_prize_exposure_log".equals(table)){
				lines.add(shake_prize_exposure_log(document));
			}else if("shake_operation_log".equals(table)){
				lines.add(shake_operation_log(document));
			}
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
			SQLUtil.createVerfFile(file, file.getName(), verfFile);
			SQLUtil.zipFile(file,filePath+CommonConstants.DEFAULT_ZIP_EXTENSION);
			FileUtils.forceDelete(file);
		}
		System.out.println("lines total: "+count);
	}
	
	public static String shake_prizebag_click_log(Document document) throws Exception{
		//select id,pv_id,prize_id,qr_code,page_url,user_id,user_ip,click_time,create_time from shake_prizebag_click_log 
		//where create_time>='' and create_time<''
		String id=document.get("id")==null?"":document.get("id").toString();
	    String pv_id=document.get("pv_id")==null?"":document.get("pv_id").toString();
	    String prize_id=document.get("prize_id")==null?"":document.get("prize_id").toString();
	    String qr_code=document.get("qr_code")==null?"":document.get("qr_code").toString();
	    String page_url=document.get("page_url")==null?"":document.get("page_url").toString();
	    String user_id=document.get("user_id")==null?"":document.get("user_id").toString();
	    String user_ip=document.get("user_ip")==null?"":document.get("user_ip").toString();
	    String click_time=document.get("click_time")==null?"":document.get("click_time").toString();
	    String create_time=document.get("create_time")==null?"":document.get("create_time").toString();
	    String line=id+"\t"+pv_id+"\t"+prize_id+"\t"+qr_code+"\t"+page_url+"\t"+user_id+"\t"+user_ip+"\t"+click_time+"\t"+create_time;
	    return line;
	}
	
	public static String shake_prizebag_exposure_log(Document document) throws Exception{
		//select id,pv_id,prize_id,qr_code,page_url,user_id,user_ip,exposure_time,create_time 
		//from shake_prizebag_exposure_log where create_time>='rpfield_start_date' and create_time<'rpfield_end_date'
		String id=document.get("id")==null?"":document.get("id").toString();
	    String pv_id=document.get("pv_id")==null?"":document.get("pv_id").toString();
	    String prize_id=document.get("prize_id")==null?"":document.get("prize_id").toString();
	    String qr_code=document.get("qr_code")==null?"":document.get("qr_code").toString();
	    String page_url=document.get("page_url")==null?"":document.get("page_url").toString();
	    String user_id=document.get("user_id")==null?"":document.get("user_id").toString();
	    String user_ip=document.get("user_ip")==null?"":document.get("user_ip").toString();
	    String exposure_time=document.get("exposure_time")==null?"":document.get("exposure_time").toString();
	    String create_time=document.get("create_time")==null?"":document.get("create_time").toString();
	    String line=id+"\t"+pv_id+"\t"+prize_id+"\t"+qr_code+"\t"+page_url+"\t"+user_id+"\t"+user_ip+"\t"+exposure_time+"\t"+create_time;
	    return line;
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
	    String create_time=document.get("create_time")==null?"":document.get("create_time").toString();
	    String line=id+"\t"+activity_id+"\t"+start_time+"\t"+end_time+"\t"+code+"\t"+qr_code+"\t"+ticket+"\t"+open_id+"\t"+uuid
	    		+"\t"+major+"\t"+minor+"\t"+page_id+"\t"+create_time;
	    return line;
	}
	
	public static String shake_prize_close_log(Document document) throws Exception{
		//select id,pv_id,prize_id,qr_code,page_url,user_id,user_ip,close_time,create_time from shake_prize_close_log 
		//where create_time>='rpfield_start_date' and create_time<'rpfield_end_date'
		String id=document.get("id")==null?"":document.get("id").toString();
	    String pv_id=document.get("pv_id")==null?"":document.get("pv_id").toString();
	    String prize_id=document.get("prize_id")==null?"":document.get("prize_id").toString();
	    String qr_code=document.get("qr_code")==null?"":document.get("qr_code").toString();
	    String page_url=document.get("page_url")==null?"":document.get("page_url").toString();
	    String user_id=document.get("user_id")==null?"":document.get("user_id").toString();
	    String user_ip=document.get("user_ip")==null?"":document.get("user_ip").toString();
	    String close_time=document.get("close_time")==null?"":document.get("close_time").toString();
	    String create_time=document.get("create_time")==null?"":document.get("create_time").toString();
	    String line=id+"\t"+pv_id+"\t"+prize_id+"\t"+qr_code+"\t"+page_url+"\t"+user_id+"\t"+user_ip+"\t"+close_time+"\t"+create_time;
	    return line;
	}
	
	public static String shake_effective_exposure_log(Document document) throws Exception{
		//select id,pv_id,query_id,prize_id,qr_code,page_url,user_id,user_ip,exposure_time,create_time 
		//from shake_effective_exposure_log where create_time>='rpfield_start_date' and create_time<'rpfield_end_date'
		String id=document.get("id")==null?"":document.get("id").toString();
	    String pv_id=document.get("pv_id")==null?"":document.get("pv_id").toString();
	    String query_id=document.get("query_id")==null?"":document.get("query_id").toString();
	    String prize_id=document.get("prize_id")==null?"":document.get("prize_id").toString();
	    String qr_code=document.get("qr_code")==null?"":document.get("qr_code").toString();
	    String page_url=document.get("page_url")==null?"":document.get("page_url").toString();
	    String user_id=document.get("user_id")==null?"":document.get("user_id").toString();
	    String user_ip=document.get("user_ip")==null?"":document.get("user_ip").toString();
	    String exposure_time=document.get("exposure_time")==null?"":document.get("exposure_time").toString();
	    String create_time=document.get("create_time")==null?"":document.get("create_time").toString();
	    String line=id+"\t"+pv_id+"\t"+query_id+"\t"+prize_id+"\t"+qr_code+"\t"+page_url+"\t"+user_id+"\t"+user_ip+"\t"+exposure_time+"\t"+create_time;
	    return line;
	}
	
	public static String shake_query_prize_log(Document document) throws Exception{
		//select id,pv_id,user_id,user_ip,query_url,activity_id,beacon_id,query_time,create_time from shake_query_prize_log 
		//where create_time>='rpfield_start_date' and create_time<'rpfield_end_date'
		String id=document.get("id")==null?"":document.get("id").toString();
	    String pv_id=document.get("pv_id")==null?"":document.get("pv_id").toString();
	    String user_id=document.get("user_id")==null?"":document.get("user_id").toString();
	    String user_ip=document.get("user_ip")==null?"":document.get("user_ip").toString();
	    String query_url=document.get("query_url")==null?"":document.get("query_url").toString();
	    String activity_id=document.get("activity_id")==null?"":document.get("activity_id").toString();
	    String beacon_id=document.get("beacon_id")==null?"":document.get("beacon_id").toString();
	    String query_time=document.get("query_time")==null?"":document.get("query_time").toString();
	    String create_time=document.get("create_time")==null?"":document.get("create_time").toString();
	    String line=id+"\t"+pv_id+"\t"+user_id+"\t"+user_ip+"\t"+query_url+"\t"+activity_id+"\t"+beacon_id+"\t"+query_time+"\t"+create_time;
	    return line;
	}
	
	public static String shake_prize_exposure_log(Document document) throws Exception{
		//select id,pv_id,prize_id,qr_code,page_url,user_id,user_ip,exposure_time,create_time from shake_prize_exposure_log 
		//where create_time>='rpfield_start_date' and create_time<'rpfield_end_date'
		String id=document.get("id")==null?"":document.get("id").toString();
	    String pv_id=document.get("pv_id")==null?"":document.get("pv_id").toString();
	    String prize_id=document.get("prize_id")==null?"":document.get("prize_id").toString();
	    String qr_code=document.get("qr_code")==null?"":document.get("qr_code").toString();
	    String page_url=document.get("page_url")==null?"":document.get("page_url").toString();
	    String user_id=document.get("user_id")==null?"":document.get("user_id").toString();
	    String user_ip=document.get("user_ip")==null?"":document.get("user_ip").toString();
	    String exposure_time=document.get("exposure_time")==null?"":document.get("exposure_time").toString();
	    String create_time=document.get("create_time")==null?"":document.get("create_time").toString();
	    String line=id+"\t"+pv_id+"\t"+prize_id+"\t"+qr_code+"\t"+page_url+"\t"+user_id+"\t"+user_ip+"\t"+exposure_time+"\t"+create_time;
	    return line;
	}
	
	public static String shake_operation_log(Document document) throws Exception{
		//select id,user_id,operation_time,operation_type,operation_name,operation_content,operation_content_before,
		//operation_content_after,create_time,operation_product,is_app 
		//from shake_operation_log where create_time>='rpfield_start_date' and create_time<'rpfield_end_date'
		String id=document.get("id")==null?"":document.get("id").toString();
	    String user_id=document.get("user_id")==null?"":document.get("user_id").toString();
	    String operation_time=document.get("operation_time")==null?"":document.get("operation_time").toString();
	    String operation_type=document.get("operation_type")==null?"":document.get("operation_type").toString();
	    String operation_name=document.get("operation_name")==null?"":document.get("operation_name").toString();
	    String operation_content=document.get("operation_content")==null?"":document.get("operation_content").toString();
	    String operation_content_before=document.get("operation_content_before")==null?"":document.get("operation_content_before").toString();
	    String operation_content_after=document.get("operation_content_after")==null?"":document.get("operation_content_after").toString();
	    String create_time=document.get("create_time")==null?"":document.get("create_time").toString();
	    String operation_product=document.get("operation_product")==null?"":document.get("operation_product").toString();
	    String is_app=document.get("is_app")==null?"":document.get("is_app").toString();
	    String line=id+"\t"+user_id+"\t"+operation_time+"\t"+operation_type+"\t"+operation_name+"\t"+operation_content
	    		+"\t"+operation_content_before+"\t"+operation_content_after+"\t"+create_time+"\t"+operation_product+"\t"+is_app;
	    return line;
	}
	
	
	
}
