package cn.rtmap.flume.source.sftp;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVWriter;
import cn.rtmap.flume.scheduler.MyJob;
import cn.rtmap.flume.scheduler.SimpleScheduler;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

public class SFTPSource extends AbstractSource implements Configurable, PollableSource {
	private static final Logger LOG = LoggerFactory.getLogger(SFTPSource.class);

	private SFTPOperator ftp;
	private SFTPSourceHelper sourceHelper;
	private CSVWriter csvWriter;
	private SimpleScheduler sch;
	private MyJob job;

	private static final int SLEEP_INTERVAL = 1 * 1000;
	
	@Override
	public Status process() throws EventDeliveryException {
		try {
			if (job.isTriggered()) {
				ftp = new SFTPOperator();
				ftp.login();

				List<HashMap> itemList = ftp.getItemList();
				List<DataFile> dfs = ftp.getDataFileList(itemList);

				List<String> dirList = new ArrayList<String>();
				for (DataFile df : dfs) {
					if (!dirList.contains(df.getFilePath())) {
						dirList.add(df.getFilePath());
					}

					try {
						csvWriter = new CSVWriter(new ChannelWriter(df), '\t',CSVWriter.NO_QUOTE_CHARACTER);
						csvWriter.writeAll(df.getAllRows());
						csvWriter.flush();
					} finally {
						if (csvWriter != null)
							csvWriter.close();
					}
				}
				
				for (String dir : dirList) {
					ftp.backup(dir);
				}
				job.setSignal(false);
			}
    		Thread.sleep(SLEEP_INTERVAL);
    		return Status.READY;
    	} catch (IOException | JSchException | InterruptedException | SftpException e) {
    		LOG.error("sftp failed", e);
    		return Status.BACKOFF;
    	} finally {
    		if (ftp != null)
    			ftp.logout();
    	}
	}

	@Override
	public void configure(Context context) {
		sourceHelper = new SFTPSourceHelper(context);
		sch = new SimpleScheduler();
		job = new MyJob();
	}

	@Override
    public void start() {
        LOG.info("Starting sftp source {} ...", getName());
        super.start();
		sch.scheduleJob(sourceHelper.getCornScheduleExpress(), sourceHelper.getCornScheduleJobKey(), sourceHelper.getCornScheduleTriggerKey());
    }

    @Override
    public void stop() {
        LOG.info("Stopping sftp source {} ...", getName());
        super.stop();
    }

    private class ChannelWriter extends Writer{
        private List<Event> events = new ArrayList<>();
        private DataFile dataFile;

        public ChannelWriter(DataFile dataFile) {
        	this.dataFile = dataFile;
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            Event event = new SimpleEvent();
            String s = new String(cbuf);
            event.setBody(s.substring(off, len-1).getBytes());

            Map<String, String> headers;
            headers = new HashMap<String, String>();
            headers.put("filename", dataFile.getFileName());
            headers.put("ext", dataFile.getExtName());
            headers.put("type", dataFile.getDataType());
            headers.put("timestamp", String.valueOf(System.currentTimeMillis()));

            event.setHeaders(headers);
            events.add(event);
            flush();
        }

        @Override
        public void flush() throws IOException {
            getChannelProcessor().processEventBatch(events);
            events.clear();
        }

        @Override
        public void close() throws IOException {
            flush();
        }
    }
}
