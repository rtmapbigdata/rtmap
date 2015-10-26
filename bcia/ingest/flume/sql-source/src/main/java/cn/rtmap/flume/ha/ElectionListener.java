package cn.rtmap.flume.ha;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElectionListener extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(ElectionListener.class);

    private volatile boolean isTerminated = false;
    private Master m;

    public ElectionListener(Master m) {
        this.m = m;
    }

    public boolean isTerminated() {
        return isTerminated;
    }
/*
    public static void main(String[] args) throws Exception {
        String zkList = "datanode1:2181,datanode2:2181,datanode3:2181";
        String path = "/master";
        int timeout = 15000;

        Master m = new Master(zkList, path, timeout);
        ElectionListener listener = new ElectionListener(m);

        listener.start();
        while (true) {
            Thread.sleep(1000);
            LOG.info("Is leader: {}", m.isLeader());
        }
    }
*/
    public void run() {
        try {
            m.startZK();
            while (!m.isConnected()) {
                Thread.sleep(100);
            }
            m.runForMaster();

            while (!m.isExpired()) {
                Thread.sleep(1000);
            }
            m.stopZK();
        } catch (IOException | InterruptedException e) {
            LOG.error("Error on detecting master from zookeeper", e);
        }
        isTerminated = true;
    }
}
