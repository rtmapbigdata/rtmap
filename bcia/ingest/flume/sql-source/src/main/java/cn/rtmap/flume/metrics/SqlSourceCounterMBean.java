package cn.rtmap.flume.metrics;

public interface SqlSourceCounterMBean {
    public long getEventCount();
    public void incrementEventCount(int value);
    public long getAverageThroughput();
    public long getCurrentThroughput();
    public long getMaxThroughput();
}
