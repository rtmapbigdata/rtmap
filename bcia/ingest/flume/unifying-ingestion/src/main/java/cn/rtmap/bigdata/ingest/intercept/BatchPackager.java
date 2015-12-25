package cn.rtmap.bigdata.ingest.intercept;

import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.impl.HeaderConstants;

public class BatchPackager extends AbstractFlumeInterceptor {
	static final Logger logger = LoggerFactory.getLogger(BatchPackager.class);
	
	static final String CONFIG_BATCH_SIZE = "batchSize";
	static final String CONFIG_ROW_DELIMITER = "rowDelimiter";
	
	static final int DEFAULT_BATCH_SIZE = 256;
	static final String DEFAULT_ROW_DELIMITER = "\n";
	StringBuffer buf;

	@Override
	public void initialize() {
		buf = new StringBuffer();
	}

	@Override
	public Event intercept(Event event) {
		Map<String, String> headers = event.getHeaders();
		byte[] body = event.getBody();

		long rowNum = Long.parseLong(headers.get(HeaderConstants.DEF_ROW_NUM));
		String mode = headers.get(HeaderConstants.DEF_MODE);

		if (HeaderConstants.VAL_MODE_DATA.equals(mode) && rowNum % DEFAULT_BATCH_SIZE == 0) {
			buf.append(new String(body));
			body = buf.toString().getBytes();
			headers.remove(HeaderConstants.DEF_ROW_NUM);
			headers.put(HeaderConstants.DEF_BATCH_SIZE, String.valueOf(DEFAULT_BATCH_SIZE));

			// clean the string buffer
			buf.setLength(0);
			return EventBuilder.withBody(body, headers);
		} else if (HeaderConstants.VAL_MODE_DATA.equals(mode)) {
			buf.append(new String(body));
			buf.append(DEFAULT_ROW_DELIMITER);
			return null;
		} else if (HeaderConstants.VAL_MODE_FIN.equals(mode)) {
			body = buf.toString().getBytes();
			headers.remove(HeaderConstants.DEF_ROW_NUM);
			headers.put(HeaderConstants.DEF_MODE, HeaderConstants.VAL_MODE_DATA);
			headers.put(HeaderConstants.DEF_BATCH_SIZE, String.valueOf(rowNum % DEFAULT_BATCH_SIZE));
			buf.setLength(0);
			return EventBuilder.withBody(body, headers);
		} else {
			return event;
		}
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
		// NOOP
	}

	public static class Builder implements Interceptor.Builder {
		@Override
		public void configure(Context context) {
			// NOPE
		}

		@Override
		public Interceptor build() {
			return new BatchPackager();
		}
	}
}
