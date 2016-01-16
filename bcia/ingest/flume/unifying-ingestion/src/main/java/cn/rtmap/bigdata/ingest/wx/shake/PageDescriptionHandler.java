package cn.rtmap.bigdata.ingest.wx.shake;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.utils.SQLOperator;

public class PageDescriptionHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(PageDescriptionHandler.class);
	private SQLOperator sqlOpterator;
	private String sql;
	//PageDescriptor page;
	
	public PageDescriptionHandler(SQLOperator sqlOpterator, String sql) {
		this.sql = sql;
		this.sqlOpterator = sqlOpterator;
	}

	public Iterator<PageDescription> getPageDescription() {
		List<PageDescription> list = new ArrayList<PageDescription>();
		try {
			ResultSet res = sqlOpterator.executeQuery(sql);
			while (res.next()) {
				PageDescription p = new PageDescription();
				p.setId(res.getLong("id"));
				p.setTitle(res.getString("title"));
				p.setDescription(res.getString("description"));
				p.setComment(res.getString("comment"));
				p.setCreateTime(res.getString("create_time"));
				list.add(p);
			}
		} catch (ClassNotFoundException | SQLException e) {
			LOGGER.error("Get page description failed", e);
		} finally {
			sqlOpterator.close();
		}
		return list.iterator();
	}
}
