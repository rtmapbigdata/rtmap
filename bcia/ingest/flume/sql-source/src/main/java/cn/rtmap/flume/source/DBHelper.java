package cn.rtmap.flume.source;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.flume.dao.OracleOperator;

public class DBHelper {
	private static final Logger LOG = LoggerFactory.getLogger(DBHelper.class);
	private SQLSourceHelper sqlSourceHelper;
	private OracleOperator oraHelper;
	
	public DBHelper(SQLSourceHelper sqlSourceHelper) {
		this.sqlSourceHelper = sqlSourceHelper;
		oraHelper = new OracleOperator();
	}

	public List<List<Object>> executeQuery() {
		try {
			List<List<Object>> rowList = new ArrayList<List<Object>>();
			
			ResultSet rs = oraHelper.executeQuery(sqlSourceHelper.getQuery());
			ResultSetMetaData meta = rs.getMetaData();
			int num = meta.getColumnCount();

			while (rs.next()) {
				List<Object> row = new ArrayList<Object>();
				for (int i = 1; i <= num; ++i) {
					row.add(rs.getObject(i));
				}
				rowList.add((ArrayList<Object>) row);
			}
			return rowList;
		} catch (ClassNotFoundException | SQLException e) {
			LOG.error("execute sql error", e);
			return null;
		}
	}

	public String GetLastRowIndex() {
		try {
			ResultSet rs = oraHelper.executeQuery(sqlSourceHelper.getIndexQuery());
			Object idx = null;
			while (rs.next()) {
				idx = rs.getObject(1);
				break;
			}
			return (idx != null) ? idx.toString() : null;
		} catch (ClassNotFoundException | SQLException e) {
			LOG.error("execute query error", e);
			return null;
		}
	}

	public void close() {
		oraHelper.close();
	}
}
