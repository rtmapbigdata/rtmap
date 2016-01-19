package cn.rtmap.bigdata.ingest.ruian;

import java.util.List;

public class TestData {

	private String cityid;

	private String place_desc;

	private List<Data> datas;

	public String getCityid() {
		return cityid;
	}

	public void setCityid(String cityid) {
		this.cityid = cityid;
	}

	public String getPlace_desc() {
		return place_desc;
	}

	public void setPlace_desc(String place_desc) {
		this.place_desc = place_desc;
	}

	public List<Data> getDatas() {
		return datas;
	}

	public void setDatas(List<Data> datas) {
		this.datas = datas;
	}

}
