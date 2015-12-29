package cn.rtmap.bigdata.ingest.agent;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HTTP;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.rtmap.bigdata.ingest.impl.HeaderConstants;
import cn.rtmap.bigdata.ingest.utils.Compressor;
import cn.rtmap.bigdata.ingest.utils.HexStringUtil;

public class PositionDataService {
	private static final int BATCH_LINES = 1024 * 100;
	private static final String SUFFIX_CSV = ".csv";
	private static final String UNIT_CODE = "nps";
	private static final String SRC_FROM = "lbs";
    private static final Logger LOG = LoggerFactory.getLogger(PositionDataService.class);

    public void sendFiles(String root, String date, String buildid, String url) throws IOException {
        File bids = new File(root);
        File[] bidDirs = bids.listFiles();
        if (date == null) {
            date = DateUtils.getDateByCondition(-1);
        }

        for (File bidDir : bidDirs) {
            if (!bidDir.getName().startsWith("86")) {
                continue;
            }
            if (buildid != null && !bidDir.getName().equals(buildid)) {
                continue;
            }
            try {
                LOG.info("begin load dir : " + bidDir.getName());
                for (File dateDir : bidDir.listFiles()) {
                    if (!dateDir.getName().equals(date)) {
                        continue;
                    }
                    LOG.info("date : " + dateDir.getName());
                    for (File dataFile : dateDir.listFiles()) {
                    	if (dataFile.isFile() && dataFile.getName().endsWith(SUFFIX_CSV)) {
                    		postFile(bidDir.getName(), dateDir.getName(), dataFile.getAbsolutePath(), url);
                    	}
                    }

                    String zipFileName = dateDir.getAbsolutePath() + "/" + dateDir.getName() + ".zip";
                    //System.out.println(zipFileName);
                    boolean isZip = this.zipDir(dateDir, zipFileName);
                    if (isZip) {
                    	AgentUtils.deleteCsvFiles(dateDir.getAbsolutePath());
                    	LOG.info("csv deleted !!");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public boolean zipDir(File dir, String zipfile) throws Exception {
        boolean bf = true;
        
        File file = new File(zipfile);
        if (!file.exists()) {
            file.createNewFile();
        }else{
        	return true;
        }

        FileOutputStream out = new FileOutputStream(zipfile);
        ZipOutputStream zipOut = new ZipOutputStream(out);
        if (dir == null) {
            zipOut.close();
            return false;
        }

        int count = 0;
        for (File f : dir.listFiles()) {
            try {
                if (f.getName().endsWith(".zip")) {
                    continue;
                }
                count++;

                FileInputStream in = new FileInputStream(f);

                String fileName = f.getName();

                ZipEntry entry = new ZipEntry(fileName);
                zipOut.putNextEntry(entry);

                int nNumber = 0;
                byte[] buffer = new byte[4096];
                while ((nNumber = in.read(buffer)) != -1) {
                    zipOut.write(buffer, 0, nNumber);
                }

                in.close();
            } catch (IOException e) {
                e.printStackTrace();
                bf = false;
            }
        }
        //LOG.info("zip files : " + count);
        //System.out.println("zip files : " + count);
        zipOut.close();
        out.close();
        return bf;
    }

    private void postFile(String buildid, String date, String fileName, String url) {
    	long rowId = 0;
    	boolean isCompleted = false;

    	while (!isCompleted) {
    		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
    			String line;
    			long idx = 0;
    			while (rowId > 0 && ++idx <= rowId) {
    				br.readLine();
    			}
    			StringBuffer sb = new StringBuffer();
    			while ((line = br.readLine()) != null) {
    				++rowId;
    				sb.append(String.format("%s\n", line));
    				if (rowId % BATCH_LINES == 0) break;
    			}
    			isCompleted = (line == null) ? true : false;

    			JSONObject obj = new JSONObject();
    	    	Map<String, String> map = new HashMap<String,String>();

    	    	byte[] content = Compressor.compress(sb.toString().getBytes());
    	    	String hexStr = HexStringUtil.toHexString(content);
    	    	//System.out.println(hexStr);
    	    	//System.out.println(new String(Compressor.decompress(HexStringUtil.toByteArray(hexStr))));
    	    	obj.put("body", hexStr);

    	    	map.put(HeaderConstants.DEF_MODE, HeaderConstants.VAL_MODE_DATA);
    	    	map.put(HeaderConstants.DEF_FROM, SRC_FROM);
    	    	map.put(HeaderConstants.DEF_UNIT_CODE, UNIT_CODE);

    	    	String yyyymmdd = date.replace("-", "");
    	    	map.put(HeaderConstants.DEF_PROCESS_MONTH, yyyymmdd.substring(0, "YYYYMM".length()));
    	    	map.put(HeaderConstants.DEF_PROCESS_DATE, yyyymmdd);
    	    	map.put(HeaderConstants.DEF_FILENAME, String.format("i_%s_%s", yyyymmdd, buildid));
    	    	map.put(HeaderConstants.DEF_COMPRESS, HeaderConstants.VAL_COMPRESS_GZIP);
    	    	map.put(HeaderConstants.DEF_ENCODE, HeaderConstants.VAL_ENCODE_HEX);
    	    	obj.put("headers", map);

    	    	JSONArray arr = new JSONArray();
    	    	arr.put(0, obj);

    	    	postData(arr.toString(), url);
    		} catch (IOException e) {
    			LOG.error("Read file content failed", e);
    			return;
    		}
    	}
    }
    
    private void postData(String content, String url) {
    	StringEntity entity = new StringEntity(content, HTTP.UTF_8);
    	entity.setContentType("application/json");

    	HttpPost httpPost = new HttpPost(url);
    	httpPost.setEntity(entity);
    	
    	HttpClient client = new DefaultHttpClient();
    	try {
			HttpResponse response = client.execute(httpPost);
			if (HttpStatus.SC_OK != response.getStatusLine().getStatusCode()) {
				LOG.error("send json over http client failed.");
			}
		} catch (IOException e) {
			LOG.error("send json over http client failed", e);
			//e.printStackTrace();
		}
    }

//    public static void main(String[] args) throws IOException {
//    	PositionDataService service = new PositionDataService();
//
//    	String url = "http://101.200.144.241:41414";
//    	service.sendFiles("D:/work/hongkgc_30005", "2015-08-26", null, url);
//    }
}
