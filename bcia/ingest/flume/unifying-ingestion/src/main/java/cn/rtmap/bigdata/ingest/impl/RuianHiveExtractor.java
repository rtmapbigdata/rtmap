package cn.rtmap.bigdata.ingest.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.base.Extractor;
import cn.rtmap.bigdata.ingest.base.JsonElement;
import cn.rtmap.bigdata.ingest.constant.CommonConstants;
import cn.rtmap.bigdata.ingest.constant.DBConstants;
import cn.rtmap.bigdata.ingest.ruian.Action;
import cn.rtmap.bigdata.ingest.ruian.Data;
import cn.rtmap.bigdata.ingest.ruian.TestData;
import cn.rtmap.bigdata.ingest.utils.DateUtils;
import cn.rtmap.bigdata.ingest.utils.SQLUtil;

public class RuianHiveExtractor implements Extractor {
	private static final Logger logger = LoggerFactory.getLogger(RuianHiveExtractor.class);
	private Connection connection = null;
	private Statement statement = null;
	private int threshold = 200;
	private boolean isFinish = false;
	private boolean debugger = false;
	private String date = "";
	
	@Override
	public void init(Context ctx) {
		String driver=ctx.getString(DBConstants.CONFIG_JDBC_DRIVER,"").trim();
		String url=ctx.getString(DBConstants.CONFIG_CONNECTION_URL,"").trim();
		String username=ctx.getString(DBConstants.CONFIG_CONNECTION_USERNAME,"").trim();
		String password=ctx.getString(DBConstants.CONFIG_CONNECTION_PASSWORD,"").trim();
		debugger=ctx.getBoolean(CommonConstants.CONFIG_DEBUG,false);
		date=ctx.getString(CommonConstants.CONFIG_RECOVER_DATE,"").trim();
		try {
			connection = SQLUtil.getConnection(driver,url,username,password);
			statement = connection.createStatement();
			logger.info("ruian hive extractor init finish");
		} catch (Exception e) {
			logger.error("create connection or statement error: "+e.getLocalizedMessage(),e);
		}
	}

	@Override
	public boolean prepare() {
		return !isFinish;
	}

	@Override
	public Iterator<JsonElement<String, String>> getData() {
		isFinish=true;
		if(debugger){
			logger.info("debugger running with date : "+date);
			if(StringUtils.isBlank(date)){
				return null;
			}
		}else{
			date = DateUtils.getDateByCondition(-1);
		}
		String day_id=date.replaceAll("-", "");
		String month_id=day_id.substring(0, 6);
		String sql="select mac,max(loc_time) as loc_time from (" 
		           + "select mac,substr(loc_time,0,15) as dt,loc_time from ods_lbs_net_position_yyyymmdd "
		           + "where month_id="+month_id+" and day_id="+day_id+" and build_id=860100010030100003"
				   + ") t1 group by mac,dt";
		//String sql="select mac,loc_time from ods_lbs_net_position_yyyymmdd where month_id=201601 and day_id=20160121 limit 20";
		logger.info("starting get data: "+sql);
		List<JsonElement<String, String>> res = new LinkedList<JsonElement<String, String>>();
		Map<String, List<String>> datas=new HashMap<>();//<mac,times>
		try {
			ResultSet rs=statement.executeQuery(sql);
			String mac = null;
			String loc_time = null;
			while(rs.next()){
				mac=rs.getString("mac");
				loc_time=rs.getString("loc_time");
				if(!datas.containsKey(mac)){
					datas.put(mac, new ArrayList<String>());
				}
				datas.get(mac).add(loc_time);
			}
			logger.info("mac count is " + datas.size());
			Map<String, String> headers=new HashMap<>();
			List<String> jsons=toHttpBody(datas);
			logger.info("event count is " + jsons.size());
			datas.clear();
			for(String json:jsons){
				JsonElement<String, String> o = new JsonElement<String, String>();
				o.setHeaders(headers);
				o.setBody(json.getBytes());
			    res.add(o);
			}
			jsons.clear();
		} catch (Exception e) {
			logger.error("extractor get data error,"+e.getLocalizedMessage(),e);
		}
		return res.iterator();
	}
	
	private List<String> toHttpBody(Map<String, List<String>> datas) throws Exception{
		List<String> jsons = new ArrayList<String>();
		List<Data> dataList = new ArrayList<Data>();
		for (String mac:datas.keySet()) {
			List<Action> actionsList = new ArrayList<Action>();
			for (String dt:datas.get(mac)) {
				String arr[] = dt.split(" ");
				Action action = new Action();
				action.setTimestamp(arr[0] + "T" + arr[1] + ".000Z");
				actionsList.add(action);
			}
			Data data = new Data();
			data.setMac(mac);
			data.setActions(actionsList);
			dataList.add(data);
			if(dataList.size() >= threshold){
				jsons.add(toJson(dataList));
				dataList.clear();
			}
		}
		if(dataList.size() > 0){
			jsons.add(toJson(dataList));
			dataList.clear();
		}
		return jsons;
	}
	
	private String toJson(List<Data> dataList) throws Exception{
		TestData testData = new TestData();
		testData.setCityid("110000");
		testData.setDatas(dataList);
		testData.setPlace_desc("");
		ObjectMapper mapper=new ObjectMapper();
		String json=mapper.writeValueAsString(testData);
		return json;
	}
	
	@Override
	public void cleanup() {
		SQLUtil.close(connection, statement);
	}

	public static void main(String[] args) throws InterruptedException {
		Connection connection = null;
		Statement statement = null;
		try {
			connection = SQLUtil.getConnection("org.apache.hive.jdbc.HiveDriver", 
					"jdbc:hive2://rtmap-data-01:10000/rtmap_lbs", "rtg", "");
			statement = connection.createStatement();
			String sql="select mac,loc_time from ods_lbs_net_position_yyyymmdd where month_id=201601 and day_id=20160117 and build_id=860100010030100003 limit 10";
			ResultSet rs=statement.executeQuery(sql);
			SQLUtil.saveResultSetAsCSV(rs, "C:\\Users\\zxw\\Desktop\\temp\\hive.csv");
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			SQLUtil.close(connection, statement);
		}
	}
}
