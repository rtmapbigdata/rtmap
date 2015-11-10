package cn.rtmap.flume.validation;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LbsTimestampAdder implements Interceptor {
	private static final Logger LOG = LoggerFactory.getLogger(LbsTimestampAdder.class);

	public boolean validate(Object data) {
		return true;
	}

	@Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody());

        /* append a timestamp column */
		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date();

		StringBuffer buf = new StringBuffer();
        String[] rows = body.toString().split("\n");
        String dateStr = sdf.format(date);
        for (int i = 0; i < rows.length; ++i) {
        	String row = String.format("%s\t%s\n", rows[i], dateStr);
        	buf.append(row);
        }
		body = buf.toString();

        if (!validate(body)) {
            LOG.error("data validation failed: {}", body);
            headers.put("validation", "1");
        } else {
            headers.put("validation", "0");
        }

        event.setBody(body.getBytes());
        return event;
    }

	@Override
	public void initialize() {}

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

	@Override
	public void close() {}

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new LbsTimestampAdder();
        }

        @Override
        public void configure(Context context) {}
    }
}
