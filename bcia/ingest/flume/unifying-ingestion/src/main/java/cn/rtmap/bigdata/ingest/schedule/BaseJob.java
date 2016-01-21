package cn.rtmap.bigdata.ingest.schedule;

import java.util.Map;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import cn.rtmap.bigdata.ingest.base.JsonElement;

public class BaseJob {
	public Event buildEvent(JsonElement<String, String> o) {
		byte[] body = o.getBody();
		Map<String, String> headers = o.getHeaders();
		// format the message
		return EventBuilder.withBody(body, headers);
	}
}
