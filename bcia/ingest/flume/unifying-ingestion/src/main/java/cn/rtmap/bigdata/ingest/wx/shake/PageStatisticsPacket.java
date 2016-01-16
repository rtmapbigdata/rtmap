package cn.rtmap.bigdata.ingest.wx.shake;

import java.util.List;

public class PageStatisticsPacket {
	private List<PageStatistics> data;
	private long errcode;
	private String errmsg;

	public List<PageStatistics> getData() {
		return data;
	}

	public void setData(List<PageStatistics> data) {
		this.data = data;
	}

	public long getErrcode() {
		return errcode;
	}

	public void setErrcode(long errcode) {
		this.errcode = errcode;
	}

	public String getErrmsg() {
		return errmsg;
	}

	public void setErrmsg(String errmsg) {
		this.errmsg = errmsg;
	}
}
