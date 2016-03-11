package cn.rtmap.bigdata.ingest.talkingdata;

public class TalkingDataTagResult {
	private String tdid;
	private TalkingDataTags tags;
	private String seq;

	public String getTdid() {
		return tdid;
	}

	public void setTdid(String tdId) {
		this.tdid = tdId;
	}

	public TalkingDataTags getTags() {
		return tags;
	}

	public void setTags(TalkingDataTags tags) {
		this.tags = tags;
	}

	public String getSeq() {
		return seq;
	}

	public void setSeq(String seq) {
		this.seq = seq;
	}
}
