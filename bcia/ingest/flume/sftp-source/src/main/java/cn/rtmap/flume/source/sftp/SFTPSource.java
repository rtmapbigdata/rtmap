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
				List<HashMap> itemList = ftp.getItemList();
				List<DataFile> dfs = ftp.getDataFileList(itemList);

				List<String> dirList = new ArrayList<String>();
				for (DataFile df : dfs) {
					if (!dirList.contains(df.getFilePath())) {
						dirList.add(df.getFilePath());
						ftp.backup(df.getFilePath());
					}
					
					try {
						csvWriter = new CSVWriter(new ChannelWriter(df), '\t',
								CSVWriter.NO_QUOTE_CHARACTER);
						csvWriter.writeAll(df.getAllRows());
						csvWriter.flush();
					} finally {
						if (csvWriter != null)
							csvWriter.close();
					}
				}
				job.setSignal(false);
			}
    		Thread.sleep(SLEEP_INTERVAL);
    		return Status.READY;
    	} catch (IOException | JSchException | InterruptedException | SftpException e) {
    		LOG.error("Error getting data from sftp", e);
    		return Status.BACKOFF;
    	}
	}

	@Override
	public void configure(Context context) {
		try {
			ftp = new SFTPOperator();
			sourceHelper = new SFTPSourceHelper(context);
			sch = new SimpleScheduler();
			job = new MyJob();
		} catch (JSchException e) {
			LOG.equals(e);
		}
	}
	
	@Override
    public void start() {
        LOG.info("Starting sftp source {} ...", getName());

        super.start();
        try {
			ftp.login();
			sch.scheduleJob(sourceHelper.getCornScheduleExpress(), sourceHelper.getCornScheduleJobKey(), sourceHelper.getCornScheduleTriggerKey());
		} catch (IOException e) {
			LOG.error("start sftp...", e);
		}
    }

    @Override
    public void stop() {
        LOG.info("Stopping sftp source {} ...", getName());
        ftp.logout();
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
