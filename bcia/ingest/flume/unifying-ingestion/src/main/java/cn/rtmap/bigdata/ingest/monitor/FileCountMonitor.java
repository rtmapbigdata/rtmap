package cn.rtmap.bigdata.ingest.monitor;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import cn.rtmap.bigdata.ingest.utils.URLUtil;
import cn.rtmap.bigdata.ingest.utils.MailSender;

public class FileCountMonitor {
	private static final Logger LOGGER = LoggerFactory.getLogger(FileCountMonitor.class);

	private final static String OP = "GETCONTENTSUMMARY";
	private final static String CRITERIA_TABLE = "table";
	private final static String CRITERIA_FILE_COUNT = "fileCount";

	private final static int retries = 10;
	private String criteriaFile;
	private String errMsg;
	private String emails;

	private final static Random rand = new Random();
	private FileCountCriteriaList criterias;

	public FileCountMonitor(String criteriaFile) {
		this.criteriaFile = criteriaFile;
	}

	private boolean checkFileCountOK() {
		parseCriteriaJson(criteriaFile);
		StringBuffer buf = new StringBuffer();

		emails = criterias.getEmails();
		for (FileCountCriteria criteria : criterias.getCriteriaList()) {
			for (HashMap<String, Object> map : criteria.getCriteria()) {
				try {
					String unitCode = (String) map.get(CRITERIA_TABLE);
					long expectFileCount = (int) map.get(CRITERIA_FILE_COUNT);

					String url = composeURL(criteria.getServers(), criteria.getPort(), criteria.getPrefix(), unitCode, OP);
					String content = URLUtil.doGet(url);
					ObjectMapper mapper = new ObjectMapper();
					
					JsonNode root = mapper.readTree(content);
					JsonNode node = root.get("ContentSummary");
					
					ContentSummary summ = mapper.readValue(node.toString(), ContentSummary.class);
					long actualFileCount = summ.getFileCount();
					
					String msg = String.format("%s/%s/%s/%s : expect file count : %s, actual file count: %s", criteria.getPrefix(), unitCode, composeYYYYMM(), composeYYYYMMDD(), expectFileCount, actualFileCount);
					if (expectFileCount != actualFileCount) {
						LOGGER.error(msg);
						String tmp = String.format("ERROR: %s\n", msg);
						buf.append(tmp);
					} else {
						LOGGER.info(msg);
					}
				} catch (IOException e) {
					LOGGER.error("check file count failed", e);
					String error = String.format("ERROR: %s\n", e.getCause());
					buf.append(error);
				}
			}
		}
		errMsg = buf.toString();
		return !(buf.length() > 0);
	}
	
	public void sendMail() throws IOException {
		String template = "Seems everything looks good!!!";
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date date = new Date();
        String title = String.format("RTMAP big data monitoring report %s", sdf.format(date));
        String content = checkFileCountOK() ? template : errMsg;

        //System.out.println(content);
        MailSender.sendTextMail(emails, title, content);
	}

	private String composeURL(String servers, int port, String prefix, String unitCode, String op) {
		String yyyymmdd = composeYYYYMMDD();
		
		String[] serverList = servers.split(",");
		String yyyymm = composeYYYYMM(yyyymmdd);
		String urlPrefix = "";

		for (int i = 0; i < retries; ++i) {
			int idx = rand.nextInt(serverList.length);
			String serv = serverList[idx];
			urlPrefix = String.format("http://%s:%s", serv, port);
			if (URLUtil.validateEndpoint(urlPrefix)) {
				break;
			}
		}
		return String.format("%s/webhdfs/v1%s/%s/%s/%s?op=%s", urlPrefix, prefix, unitCode, yyyymm, yyyymmdd, op);
	}

	private String composeYYYYMM(String yyyymmdd) {
		String yyyymm = yyyymmdd.substring(0, 6);
		return yyyymm;
	}

	private String composeYYYYMM() {
		String yyyymm = composeYYYYMMDD().substring(0, 6);
		return yyyymm;
	}

	private String composeYYYYMMDD() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);

		Date d = cal.getTime();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String yyyymmdd = sdf.format(d);
		return yyyymmdd;
	}

	private void parseCriteriaJson(String jsonFile) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			criterias = mapper.readValue(new File(jsonFile), FileCountCriteriaList.class);
		} catch (IOException e) {
			LOGGER.error("parse json failed", e);
		}
	}

	public static void main(String[] args) throws IOException {
		FileCountMonitor mon = new FileCountMonitor(args[0]);
		mon.sendMail();
	}
}
