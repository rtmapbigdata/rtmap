package cn.rtmap.bigdata.ingest.intercept;

import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import cn.rtmap.bigdata.ingest.impl.HeaderConstants;
import cn.rtmap.bigdata.ingest.utils.Compressor;

public class EventCompressor extends AbstractFlumeInterceptor {
	//static final String COMPRESS_FORMAT = "gzip";

	@Override
	public void initialize() {
		// NOPE
	}

	@Override
	public Event intercept(Event event) {
		Map<String, String> headers = event.getHeaders();
		byte[] body = Compressor.compress(event.getBody());

		headers.put(HeaderConstants.DEF_COMPRESS, HeaderConstants.VAL_COMPRESS_GZIP);
		event.setBody(body);
		return event;
	}

//	@Override
//	public List<Event> intercept(List<Event> events) {
//		for (Iterator<Event> it = events.iterator(); it.hasNext();) {
//			Event next = intercept(it.next());
//			if (next == null) {
//				it.remove();
//			}
//		}
//		return events;
//	}

	@Override
	public void close() {
		// NOPE
	}

	public static class Builder implements Interceptor.Builder {
		@Override
		public void configure(Context context) {
			// NOPE
		}

		@Override
		public Interceptor build() {
			return new EventCompressor();
		}
	}
}
