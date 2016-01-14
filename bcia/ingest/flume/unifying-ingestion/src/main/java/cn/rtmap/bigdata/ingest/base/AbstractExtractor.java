package cn.rtmap.bigdata.ingest.base;

import org.apache.flume.Context;

import cn.rtmap.bigdata.ingest.constant.CommonConstants;

public abstract class AbstractExtractor implements Extractor{
	public boolean debugger = false;
	
	@Override
	public void init(Context ctx) {
		String debug=ctx.getString(CommonConstants.CONFIG_DEBUG);
		if("true".equals(debug) || "1".equals(debug)){
			debugger=true;
		}
	}

	@Override
	public boolean prepare() {
		return true;
	}

	@Override
	public void cleanup() {}

}
