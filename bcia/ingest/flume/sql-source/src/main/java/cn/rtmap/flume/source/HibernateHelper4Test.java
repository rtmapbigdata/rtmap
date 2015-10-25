package cn.rtmap.flume.source;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HibernateHelper4Test extends HibernateHelper {

	private static final Logger LOG = LoggerFactory.getLogger(HibernateHelper4Test.class);
	private String lastRowIndex;

	private SQLSourceHelper sqlSourceHelper = null;
	public HibernateHelper4Test(SQLSourceHelper sqlSourceHelper) {
		super(sqlSourceHelper);
		this.sqlSourceHelper = sqlSourceHelper;
	}

	@Override
	public String GetLastRowIndex() {
		// refresh value for currentIndex
		sqlSourceHelper.getIndexQuery();

		String currentIndex = sqlSourceHelper.getCurrentIndex();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		int minuteToAdd = 1;

		try {
			Date d1 = df.parse(currentIndex);
			Calendar calendar = Calendar.getInstance();

			calendar.setTime(d1);
			calendar.add(Calendar.MINUTE, minuteToAdd);

			Date d2 = calendar.getTime();
			lastRowIndex = df.format(d2);

			LOG.info("debug - current row index: {}, last row index: {}", currentIndex, lastRowIndex);
			return lastRowIndex;
		} catch (ParseException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public List<List<Object>> executeQuery() {
		List<List<Object>> rowsList = super.executeQuery();

		// record the timestamp even the result is empty
		sqlSourceHelper.updateStatusFile(lastRowIndex);
		return rowsList;
	}
}
