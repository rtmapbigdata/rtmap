package cn.rtmap.bigdata.ingest.sink;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuianOutSink extends AbstractSink implements Configurable {
	private static final Logger logger = LoggerFactory.getLogger(RuianOutSink.class);

	public void configure(Context context) {
		
	}

	public Status process() throws EventDeliveryException {
		Status status = Status.READY;
		Event event = null;
		Channel ch = getChannel();
		Transaction txn = ch.getTransaction();
		try {
			txn.begin();
			event = ch.take();
			if (event != null) {
				String res=postToRuian(event.getBody());
				logger.info("http response: "+res);
			}else {
				status = Status.BACKOFF;
            }
			txn.commit();
		} catch (Throwable t) {
			txn.rollback();
			logger.error("sink process error,"+t.getLocalizedMessage(),t);
			status = Status.BACKOFF;
			if (t instanceof Error) {
				throw (Error) t;
			}
		}finally {
			txn.close();
        }
		return status;
	}

	private String postToRuian(byte[] datas) {
		String response = "";
		OutputStream os=null;
		InputStream is = null;
		BufferedReader br = null;
		try {
			URL url = new URL("https://sso.run.com:8443/exchanger/hello");
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("POST");
			con.setDoOutput(true);
			con.setDoInput(true);
			con.setConnectTimeout(60000);
			con.setReadTimeout(60000);
			os=con.getOutputStream();
			os.write(datas);
			os.flush();
			if (con.getResponseCode() != 200) {
				logger.error("HTTP Response Code error "+con.getResponseCode());
				return "error";
			}
			is = con.getInputStream();
			if (is == null) {
				logger.error("HTTP InputStream is NULL");
				return "error";
			}
			br = new BufferedReader(new InputStreamReader(is, "utf-8"));
			String line = "";
			while ((line = br.readLine()) != null) {
				response += line;
			}
		} catch (Exception e) {
			logger.error("HTTP send error," + e.getLocalizedMessage(),e);
		}finally{
			IOUtils.closeQuietly(os);
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(is);
		}
		return response;
	}
}
