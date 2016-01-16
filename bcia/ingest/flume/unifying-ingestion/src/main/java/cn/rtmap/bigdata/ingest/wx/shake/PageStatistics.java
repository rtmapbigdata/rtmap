package cn.rtmap.bigdata.ingest.wx.shake;

public class PageStatistics {
	private long click_pv;
	private long click_uv;
	private long ftime;
	private long shake_pv;
	private long shake_uv;

	public long getClick_pv() {
		return click_pv;
	}

	public void setClick_pv(long click_pv) {
		this.click_pv = click_pv;
	}

	public long getClick_uv() {
		return click_uv;
	}

	public void setClick_uv(long click_uv) {
		this.click_uv = click_uv;
	}

	public long getShake_pv() {
		return shake_pv;
	}

	public void setShake_pv(long shake_pv) {
		this.shake_pv = shake_pv;
	}

	public long getShake_uv() {
		return shake_uv;
	}

	public void setShake_uv(long shake_uv) {
		this.shake_uv = shake_uv;
	}

	public long getFtime() {
		return ftime;
	}

	public void setFtime(long ftime) {
		this.ftime = ftime;
	}
}
