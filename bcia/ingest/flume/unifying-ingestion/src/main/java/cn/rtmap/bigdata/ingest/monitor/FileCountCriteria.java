package cn.rtmap.bigdata.ingest.monitor;

import java.util.HashMap;
import java.util.List;

public class FileCountCriteria {
	private String servers;
	private int port;
	private String prefix;
	private List<HashMap<String, Object>> criteria;

	public String getServers() {
		return servers;
	}

	public void setServers(String servers) {
		this.servers = servers;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public List<HashMap<String, Object>> getCriteria() {
		return criteria;
	}

	public void setCriteria(List<HashMap<String, Object>> criteria) {
		this.criteria = criteria;
	}
}
