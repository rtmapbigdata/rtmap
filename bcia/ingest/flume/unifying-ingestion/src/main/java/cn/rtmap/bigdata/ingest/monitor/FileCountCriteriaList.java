package cn.rtmap.bigdata.ingest.monitor;

import java.util.List;

public class FileCountCriteriaList {
	private List<FileCountCriteria> criteriaList;
	private String emails;

	public List<FileCountCriteria> getCriteriaList() {
		return criteriaList;
	}

	public void setCriteriaList(List<FileCountCriteria> criteriaList) {
		this.criteriaList = criteriaList;
	}

	public String getEmails() {
		return emails;
	}

	public void setEmails(String email) {
		this.emails = email;
	}
}
