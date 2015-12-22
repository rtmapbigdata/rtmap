package cn.rtmap.bigdata.ingest.intercept;

import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import cn.rtmap.bigdata.ingest.impl.HeaderConstants;
import cn.rtmap.bigdata.ingest.utils.Compressor;

public class EventDecompressor extends AbstractFlumeInterceptor {

	@Override
	public void initialize() {
		// NOPE
	}

	@Override
	public Event intercept(Event event) {
		Map<String, String> headers = event.getHeaders();
		String compress = headers.get(HeaderConstants.DEF_COMPRESS);

		if (compress != null && HeaderConstants.VAL_COMPRESS_GZIP.equals(compress)) {
			byte[] body = Compressor.decompress(event.getBody());
			headers.remove(HeaderConstants.DEF_COMPRESS);
			event.setBody(body);			
		}
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
			return new EventDecompressor();
		}
	}

}
