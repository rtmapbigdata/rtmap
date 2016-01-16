package cn.rtmap.bigdata.ingest.wx.shake;

public class PageStatisticsParams extends BaseParam {
	private long page_id;
	private long begin_date;
	private long end_date;
	
	public long getPage_id() {
		return page_id;
	}

	public void setPage_id(long page_id) {
		this.page_id = page_id;
	}

	public long getBegin_date() {
		return begin_date;
	}

	public void setBegin_date(long begin_date) {
		this.begin_date = begin_date;
	}

	public long getEnd_date() {
		return end_date;
	}

	public void setEnd_date(long end_date) {
		this.end_date = end_date;
	}
}
