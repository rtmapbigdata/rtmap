package cn.rtmap.bigdata.ingest.ruian;

import java.util.List;

public class Data {

	private String mac;

	private List<Action> actions;

	public String getMac() {
		return mac;
	}

	public void setMac(String mac) {
		this.mac = mac;
	}

	public List<Action> getActions() {
		return actions;
	}

	public void setActions(List<Action> actions) {
		this.actions = actions;
	}

}
