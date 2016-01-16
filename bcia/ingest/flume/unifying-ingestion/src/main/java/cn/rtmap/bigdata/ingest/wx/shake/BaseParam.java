package cn.rtmap.bigdata.ingest.wx.shake;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BaseParam {
	private static final Logger LOG = LoggerFactory.getLogger(BaseParam.class);
	private ObjectMapper mapper = null;
	
	public BaseParam() {
		mapper = new ObjectMapper();
	}
	
	public String toString() {
		String json = null;
		try {
			json = mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			LOG.error("Convert to JSON string failed", e);
		}
		return json;
	}
}
