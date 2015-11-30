package cn.rtmap.flume.source;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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

import cn.rtmap.flume.metrics.SqlSourceCounter;
import cn.rtmap.flume.scheduler.MyJob;
import cn.rtmap.flume.scheduler.SimpleScheduler;
import cn.rtmap.flume.ha.ElectionListener;
import cn.rtmap.flume.ha.Master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVWriter;


/**
 * A Source to read data from a SQL database. This source ask for new data in a table each configured time.<p>
 * 
 */
public class SQLSource extends AbstractSource implements Configurable, PollableSource {

    private static final Logger LOG = LoggerFactory.getLogger(SQLSource.class);
    protected SQLSourceHelper sqlSourceHelper;
    private SqlSourceCounter sqlSourceCounter;
    private CSVWriter csvWriter; 
    //private HibernateHelper hibernateHelper;
    private DBHelper dbHelper;

    private Master m;
    private ElectionListener listener;

    private MyJob job;
    private SimpleScheduler sch;
    private static final int DEFAULT_SLEEP_INTERVAL = 1 * 1000;
    
    private static String incrIndex;

    /**
     * Configure the source, load configuration properties and establish connection with database
     */
    @Override
    public void configure(Context context) {

        LOG.info("Reading and processing configuration values for source " + getName());
        
        /* Initialize configuration parameters */
        sqlSourceHelper = new SQLSourceHelper(context);
        
        /* Initialize metric counters */
        sqlSourceCounter = new SqlSourceCounter("SOURCESQL." + this.getName());
        
        /* Establish connection with database */
        //hibernateHelper = new HibernateHelper(sqlSourceHelper);
        //hibernateHelper.establishSession();
        dbHelper = new DBHelper(sqlSourceHelper);
        
        /* Instantiate the CSV Writer */
        csvWriter = new CSVWriter(new ChannelWriter(), '\t', CSVWriter.NO_QUOTE_CHARACTER);

        String zkHosts = sqlSourceHelper.getZKHosts();
        String zkNodePath = sqlSourceHelper.getZKNodePath();
        int zkTimeout = sqlSourceHelper.getZKTimeout();

        m = new Master(zkHosts, zkNodePath, zkTimeout);
        listener = new ElectionListener(m);

        job = new MyJob();
        sch = new SimpleScheduler();
        
        incrIndex = "";
    }

    /**
     * Process a batch of events performing SQL Queries
     */
    @Override
    public Status process() throws EventDeliveryException {
        if (m.isLeader() && !listener.isTerminated() && job.isTriggered()) {
            try {
                String maxValue = dbHelper.GetLastRowIndex();
                if (maxValue != null) {
                    sqlSourceHelper.setMaxIndex(maxValue);
                    sqlSourceCounter.startProcess();

                    List<List<Object>> result = dbHelper.executeQuery();
                    String currIndex = sqlSourceHelper.getCurrentIndex();

                    if (!result.isEmpty()) {
                    	incrIndex = maxValue;
                        csvWriter.writeAll(sqlSourceHelper.getAllRows(result));
                        csvWriter.flush();

                        sqlSourceCounter.incrementEventCount(result.size());
                        sqlSourceHelper.updateStatusFile(maxValue);
                        writeCountToFile(currIndex, maxValue, result.size());
                    }
                    sqlSourceCounter.endProcess(result.size());
                }
                Thread.sleep(DEFAULT_SLEEP_INTERVAL);
                return Status.READY;
            } catch (IOException | InterruptedException e) {
                LOG.error("Error procesing row", e);
                return Status.BACKOFF;
            } finally {
                dbHelper.close();
            }
        } else {
            try {
				Thread.sleep(sqlSourceHelper.getRunQueryDelay());
			} catch (InterruptedException e) {
				LOG.error("Processing was interrupted", e);
			}
            return Status.READY;
        }
    }
 
    /**
     * Starts the source. Starts the metrics counter.
     */
    @Override
    public void start() {

        LOG.info("Starting sql source {} ...", getName());
        sqlSourceCounter.start();
        super.start();

        // identify leader
        listener.start();
        sch.scheduleJob(sqlSourceHelper.getCornScheduleExpress(), sqlSourceHelper.getCornScheduleJobKey(), sqlSourceHelper.getCornScheduleTriggerKey());
    }

    /**
     * Stop the source. Close database connection and stop metrics counter.
     */
    @Override
    public void stop() {
        LOG.info("Stopping sql source {} ...", getName());
        try 
        {
            //hibernateHelper.closeSession();
            csvWriter.close();

            listener.terminate();
            listener.join();
        } catch (IOException | InterruptedException e) {
            LOG.warn("Error occured ", e);
        } finally {
            this.sqlSourceCounter.stop();
            super.stop();
        }
    }

    private void writeCountToFile(String startDate, String endDate, int count) throws IOException {
    	String filePath = sqlSourceHelper.getRecordCounterFilePath();
    	String prefix = sqlSourceHelper.getRecordCounterFilePrefix();
    	String postfix = ".txt";

		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();

    	String fileName = String.format("%s/%s%s%s", filePath, prefix, sdf.format(date), postfix);
    	Writer writer = new FileWriter(fileName, true);

    	/*
    	List<String> list = new ArrayList<String>();
    	list.add(startDate);
    	list.add(endDate);
    	list.add(String.valueOf(count));
*/

    	writer.write(startDate + '\t');
    	writer.write(endDate + '\t');
    	writer.write(String.valueOf(count) + '\n');
    	/*
    	String[] entries = list.toArray(new String[list.size()]);
    	writer.writeNext(entries);
    	*/
    	writer.close();
    }

    private class ChannelWriter extends Writer{
        private List<Event> events = new ArrayList<>();

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            Event event = new SimpleEvent();
            String s = new String(cbuf);
            event.setBody(s.substring(off, len-1).getBytes());

            Map<String, String> headers;
            headers = new HashMap<String, String>();
            headers.put("type", "log");
            headers.put("index", incrIndex);
            headers.put("timestamp", String.valueOf(System.currentTimeMillis()));

            event.setHeaders(headers);
            events.add(event);

            if (events.size() >= sqlSourceHelper.getBatchSize())
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
