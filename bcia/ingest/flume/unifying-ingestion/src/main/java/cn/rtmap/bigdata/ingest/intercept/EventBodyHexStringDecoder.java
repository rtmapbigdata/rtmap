package cn.rtmap.bigdata.ingest.intercept;

import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.impl.HeaderConstants;
import cn.rtmap.bigdata.ingest.utils.HexStringUtil;

public class EventBodyHexStringDecoder  extends AbstractFlumeInterceptor {
	static final Logger logger = LoggerFactory.getLogger(EventBodyHexStringDecoder.class);

	@Override
	public void initialize() {}

	@Override
	public Event intercept(Event event) {
		Map<String, String> map = event.getHeaders();
		String hexflag = map.get(HeaderConstants.DEF_ENCODE);

		if (HeaderConstants.VAL_ENCODE_HEX.equals(hexflag)) {
			byte[] body = event.getBody();
			byte[] newBody = HexStringUtil.toByteArray(new String(body));

			event.setBody(newBody);
			map.remove(HeaderConstants.DEF_ENCODE);
		}
		return event;
	}

	@Override
	public void close() {}

	public static class Builder implements Interceptor.Builder {
		@Override
		public void configure(Context context) {}

		@Override
		public Interceptor build() {
			return new EventBodyHexStringDecoder();
		}
	}
}
