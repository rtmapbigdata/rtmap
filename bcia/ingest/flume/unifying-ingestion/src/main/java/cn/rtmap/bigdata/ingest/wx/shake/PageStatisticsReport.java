package cn.rtmap.bigdata.ingest.wx.shake;

import java.text.SimpleDateFormat;
import java.util.Date;

public class PageStatisticsReport {
	private PageDescription page;
	private PageStatisticsPacket packet;
	
	public PageStatisticsReport(PageDescription page, PageStatisticsPacket statis) {
		this.page = page;
		this.packet = statis;
	}
	
	public String toString() {
		String pageInfo = String.format("%s\t%s\t%s\t%s", page.getId(), page.getTitle(), page.getDescription(), page.getComment());
		String content = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		for (PageStatistics p : packet.getData()) {
			String statInfo = String.format("%s\t%s\t%s\t%s", p.getShake_uv(), p.getShake_pv(), p.getClick_uv(), p.getClick_pv());
			Date date = new Date(p.getFtime() * 1000);
			content = String.format("%s\t%s\t%s", sdf.format(date), pageInfo, statInfo);
//			
//			System.out.println("click_pv:" + p.getClick_pv());
//			System.out.println("Click_uv:" + p.getClick_uv());
//			System.out.println("ftime:" + p.getFtime());
//			System.out.println("shake_pv:" + p.getShake_pv());
//			System.out.println("shake_uv:" + p.getShake_uv());
		}
		return content;
	}
}
