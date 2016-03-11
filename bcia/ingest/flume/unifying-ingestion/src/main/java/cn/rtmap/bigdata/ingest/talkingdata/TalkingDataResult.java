package cn.rtmap.bigdata.ingest.talkingdata;

public class TalkingDataResult {
	private int status;
	private String message;
	private TalkingDataTagResult result;

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public TalkingDataTagResult getResult() {
		return result;
	}

	public void setResult(TalkingDataTagResult result) {
		this.result = result;
	}
}
