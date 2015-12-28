package cn.rtmap.bigdata.ingest.intercept;

import java.io.IOException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.base.JsonElementBodyString;

//import com.fasterxml.jackson.core.JsonParseException;
//import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EventBodyStringDeserializer extends AbstractFlumeInterceptor {
	static final Logger logger = LoggerFactory.getLogger(EventBodyStringDeserializer.class);
	ObjectMapper mapper = new ObjectMapper();

	@Override
	public void initialize() {}

	@Override
	public Event intercept(Event event) {
		byte[] src = event.getBody();
		logger.info("debug: " + new String(src));
		ObjectMapper mapper = new ObjectMapper();

		try {
			JsonElementBodyString obj = mapper.readValue(src, JsonElementBodyString.class);
			event.setHeaders(obj.getHeaders());
			event.setBody(obj.getBody());
		} catch (IOException e) {
			logger.error("deserialize error", e);
		}
		
		return event;
	}

	@Override
	public void close() {}

	public static class Builder implements Interceptor.Builder {
		@Override
		public void configure(Context context) {
			// NOPE
		}

		@Override
		public Interceptor build() {
			return new EventBodyStringDeserializer();
		}
	}

//	public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
//		String str = "{\"headers\":{\"process_date\":\"20151214\",\"filename\":\"i_20151214_flr.dat\",\"unit_code\":\"flr\"},\"body\":[56,54,50,55,48,48,48,51,48,48,51,48,51,48]}";
//		ObjectMapper mapper = new ObjectMapper();
//		JsonElement je = mapper.readValue(str.getBytes(), JsonElement.class);
//		System.out.println(new String(je.getBody()));
//		System.out.println(new String(je.getHeader("filename").toString()));
//	}
}
