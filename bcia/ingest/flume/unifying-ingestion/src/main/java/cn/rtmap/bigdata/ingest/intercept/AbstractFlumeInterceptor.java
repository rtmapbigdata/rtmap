package cn.rtmap.bigdata.ingest.intercept;

import java.util.Iterator;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public abstract class AbstractFlumeInterceptor implements Interceptor {
    @Override
    public List<Event> intercept(List<Event> events) {
        for (Iterator<Event> iterator = events.iterator(); iterator.hasNext(); ) {
            Event next =  intercept(iterator.next());
            if(next == null) {
                // remove the element
                iterator.remove();
            }

        }
        return events;
    }
}
