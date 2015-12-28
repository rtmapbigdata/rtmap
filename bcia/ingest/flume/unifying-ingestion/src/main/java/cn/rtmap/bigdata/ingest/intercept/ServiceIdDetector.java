package cn.rtmap.bigdata.ingest.intercept;

import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.impl.HeaderConstants;

public class ServiceIdDetector extends AbstractFlumeInterceptor {
	static final Logger logger = LoggerFactory.getLogger(ServiceIdDetector.class);
	static final String CONFIG_FIELD_DELIMITER = "#";

	@Override
	public void initialize() {}

	@Override
	public Event intercept(Event event) {
		Map<String, String> map = event.getHeaders();
		if (map != null) {
			String serviceId = null;
			String unitCode = map.get(HeaderConstants.DEF_UNIT_CODE);
			String fileName = map.get(HeaderConstants.DEF_FILENAME);

			String[] toks = unitCode.split(CONFIG_FIELD_DELIMITER);
			if (toks.length > 1) {
				unitCode = toks[0];
				serviceId = toks[1];
				map.put(HeaderConstants.DEF_UNIT_CODE, unitCode);
			}

			toks = fileName.split(CONFIG_FIELD_DELIMITER);
			if (toks.length > 1) {
				fileName = toks[0];
				serviceId = toks[1];
				map.put(HeaderConstants.DEF_FILENAME, fileName);
			}

			if (serviceId != null) {
				map.put(HeaderConstants.DEF_SERVICE_ID, serviceId);
			}
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
			return new ServiceIdDetector();
		}
	}
}