package cn.rtmap.flume.source.sftp;

import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public class SFTPInterceptor implements Interceptor {

	@Override
	public void initialize() {
	}

	@Override
	public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody());

        String fileName = headers.get("filename");
        String[] toks = fileName.split("_");
        
        if (toks.length >= 3) {
        	String dateTime = toks[0];
        	String date = dateTime.substring(0, 8);
        	String driver = toks[1];
        	String vehicle = toks[2].substring(0, 2);

        	headers.put("date", date);
        	headers.put("driver", driver);
        	headers.put("vehicle", vehicle);
        	
        	body = String.format("%s\t%s\t%s\t%s", dateTime, driver, vehicle, body);
        }

        event.setBody(body.getBytes());
        return event;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
	}

	@Override
	public void close() {
	}

	public static class Builder implements Interceptor.Builder {
		@Override
	    public Interceptor build() {
	        return new SFTPInterceptor();
	    }

	    @Override
	    public void configure(Context context) {}
	}
}
