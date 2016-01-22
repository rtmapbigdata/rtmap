package cn.rtmap.bigdata.ingest.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.MalformedURLException;
import javax.net.ssl.HttpsURLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URLUtil {
	private static final Logger LOG = LoggerFactory.getLogger(URLUtil.class);
	private static final String USER_AGENT = "Mozilla/5.0";

	public static boolean validateEndpoint(final String endpoint, final String port) {
		try {
			URL url = new URL(endpoint + ":" + port);
			LOG.debug(url.toString());
			URLConnection conn = url.openConnection();
			conn.connect();
			return true;
		} catch (MalformedURLException e) {
			String errMsg = "Web Service endpoint is malformed: (" + endpoint
					+ ":" + port + ")";
			LOG.error(errMsg);
			return false;
		} catch (IOException e) {
			String errMsg = "Web Service endpoint is not valid: (" + endpoint
					+ ":" + port + ")";
			LOG.error(errMsg);
			return false;
		}
	}
	
	public static boolean validateEndpoint(final String endpoint) {
		int idx = endpoint.lastIndexOf(":");
		if (idx > 0) {
			String p1 = endpoint.substring(0, idx);
			String p2 = endpoint.substring(idx+1, endpoint.length());
			
			return validateEndpoint(p1, p2);
		} else {
			return false;
		}
	}
	
	public static String doGet(String url) throws IOException {
		URL obj = new URL(url);
		HttpURLConnection conn = (HttpURLConnection) obj.openConnection();

		conn.setRequestMethod("GET");
		conn.setRequestProperty("User-Agent", USER_AGENT);

		int code = conn.getResponseCode();
		LOG.debug("Sending 'GET' request to URL:" + url);
		LOG.debug("Reponse code:" + code);
		
		BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		StringBuffer sb = new StringBuffer();
		
		String line;
		while ((line = rd.readLine()) != null) {
			sb.append(line);
		}

		rd.close();
		String result = sb.toString();

		LOG.debug(result);
		return result;
	}
	
	public static String doPost(String url, String jsonData) throws IOException {
		URL obj = new URL(url);
		HttpURLConnection  conn = (HttpURLConnection) obj.openConnection();
		
		conn.setRequestMethod("POST");
		conn.setRequestProperty("User-Agent", USER_AGENT);
		conn.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
		conn.setDoOutput(true);
		
		OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
		wr.write(jsonData);
		wr.flush();
		
		int code = conn.getResponseCode();
		LOG.debug("Sending 'POST' request to URL:" + url);
		LOG.debug("Reponse Code:" + code);
		
		String line;
		BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		
		StringBuffer buf = new StringBuffer();
		while ((line = in.readLine()) != null) {
			buf.append(line);
		}
		in.close();
		
		String result = buf.toString();
		LOG.debug(result);
		return result;
	}
	
	public static String doHttpsPost(String url, String jsonData) throws IOException {
		URL obj = new URL(url);
		HttpsURLConnection  conn = (HttpsURLConnection) obj.openConnection();
		
		conn.setRequestMethod("POST");
		conn.setRequestProperty("User-Agent", USER_AGENT);
		conn.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
		conn.setDoOutput(true);
		
		OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
		wr.write(jsonData);
		wr.flush();
		
		int code = conn.getResponseCode();
		LOG.debug("Sending 'POST' request to URL:" + url);
		LOG.debug("Reponse Code:" + code);
		
		String line;
		BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		
		StringBuffer buf = new StringBuffer();
		while ((line = in.readLine()) != null) {
			buf.append(line);
		}
		in.close();

		String result = buf.toString();
		LOG.debug(result);
		return result;
	}
}
