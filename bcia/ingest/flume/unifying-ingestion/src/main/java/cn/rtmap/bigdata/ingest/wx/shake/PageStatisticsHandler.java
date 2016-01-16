package cn.rtmap.bigdata.ingest.wx.shake;

import java.io.IOException;
import cn.rtmap.bigdata.ingest.utils.URLUtil;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PageStatisticsHandler {
	private ObjectMapper mapper;
	private PageStatisticsParams param;

	private String url;
	private String result;

	public PageStatisticsHandler(String url, PageStatisticsParams param) {
		this.url = url;
		this.param = param;
		mapper = new ObjectMapper();
	}
	
	public PageStatisticsPacket getPacket() throws JsonParseException, JsonMappingException, IOException {
		result = URLUtil.doHttpsPost(url, param.toString());
		return mapper.readValue(result.getBytes(), PageStatisticsPacket.class);
	}
}
