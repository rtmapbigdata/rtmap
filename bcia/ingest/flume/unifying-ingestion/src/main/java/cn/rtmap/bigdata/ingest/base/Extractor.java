package cn.rtmap.bigdata.ingest.base;

import java.util.Iterator;
import org.apache.flume.Context;

public interface Extractor {
	void init(Context ctx);
	boolean prepare();
	Iterator<JsonElement<String, String>> getData();
	void cleanup();
}
