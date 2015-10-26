package cn.rtmap.flume.ha;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements the master of the master-worker example we use
 * throughout the book. The master is responsible for tracking the list of
 * available workers, determining when there are new tasks and assigning
 * them to available workers. 
 * 
 * The flow without crashes is like this. The master reads the list of
 * available workers and watch for changes to the list of workers. It also
 * reads the list of tasks and watches for changes to the list of tasks.
 * For each new task, it assigns the task to a worker chosen at random.
 * 
 * Before exercising the role of master, this ZooKeeper client first needs
 * to elect a primary master. It does it by creating a /master znode. If
 * it succeeds, then it exercises the role of master. Otherwise, it watches
 * the /master znode, and if it goes away, it tries to elect a new primary
 * master.
 * 
 * The states of this client are three: RUNNING, ELECTED, NOTELECTED. 
 * RUNNING means that according to its view of the ZooKeeper state, there
 * is no primary master (no master has been able to acquire the /master lock).
 * If some master succeeds in creating the /master znode and this master learns
 * it, then it transitions to ELECTED if it is the primary and NOTELECTED
 * otherwise.
 *
 */
public class Master implements Watcher, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    /*
     * A master process can be either running for
     * primary master, elected primary master, or
     * not elected, in which case it is a backup
     * master.  
     */
    enum MasterStates {RUNNING, ELECTED, NOTELECTED};

    private volatile MasterStates state = MasterStates.RUNNING;

    MasterStates getState() {
        return state;
    }

    private Random random = new Random(this.hashCode());
    private ZooKeeper zk;
    private String hostPort;
    private String znodePath;
    private int timeout;

    private String serverId = Integer.toHexString( random.nextInt() );
    private volatile boolean connected = false;
    private volatile boolean expired = false;

    /**
     * Creates a new master instance.
     * 
     * @param hostPort
     */
    public Master(String hostPort, String znodePath, int timeout) {
        this.hostPort = hostPort;
        this.znodePath = znodePath;
        this.timeout = timeout;
    }


    /**
     * Creates a new ZooKeeper session.
     * 
     * @throws IOException
     */
    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, timeout, this);
    }

    /**
     * Closes the ZooKeeper session.
     *
     * @throws IOException
     */
    void stopZK() throws InterruptedException, IOException {
        zk.close();
    }

    /**
     * This method implements the process method of the
     * Watcher interface. We use it to deal with the
     * different states of a session. 
     * 
     * @param e new session event to be processed
     */
    public void process(WatchedEvent e) {  
        LOG.info("Processing event: " + e.toString());
        if(e.getType() == Event.EventType.None){
            switch (e.getState()) {
            case SyncConnected:
                connected = true;
                break;
            case Disconnected:
                connected = false;
                break;
            case Expired:
                expired = true;
                connected = false;
                LOG.error("Session expiration");
            default:
                break;
            }
        }
    }

    /**
     * Check if this client is connected.
     * 
     * @return boolean ZooKeeper client is connected
     */
    boolean isConnected() {
        return connected;
    }

    /**
     * Check if the ZooKeeper session has expired.
     * 
     * @return boolean ZooKeeper session has expired
     */
    boolean isExpired() {
        return expired;
    }

    /*
     **************************************
     **************************************
     * Methods related to master election.*
     **************************************
     **************************************
     */


    /*
     * The story in this callback implementation is the following.
     * We tried to create the master lock znode. If it suceeds, then
     * great, it takes leadership. However, there are a couple of
     * exceptional situations we need to take care of. 
     * 
     * First, we could get a connection loss event before getting
     * an answer so we are left wondering if the operation went through.
     * To check, we try to read the /master znode. If it is there, then
     * we check if this master is the primary. If not, we run for master
     * again. 
     * 
     *  The second case is if we find that the node is already there.
     *  In this case, we call exists to set a watch on the znode.
     */
    StringCallback masterCreateCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                checkMaster();

                break;
            case OK:
                state = MasterStates.ELECTED;
                takeLeadership();

                break;
            case NODEEXISTS:
                state = MasterStates.NOTELECTED;
                masterExists();

                break;
            default:
                state = MasterStates.NOTELECTED;
                LOG.error("Something went wrong when running for master.", 
                        KeeperException.create(Code.get(rc), path));
            }
            LOG.info("I'm " + (state == MasterStates.ELECTED ? "" : "not ") + "the leader " + serverId);
        }
    };

    void masterExists() {
        zk.exists(znodePath, 
                masterExistsWatcher, 
                masterExistsCallback, 
                null);
    }

    StatCallback masterExistsCallback = new StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                masterExists();
                
                break;
            case OK:
                break;
            case NONODE:
                state = MasterStates.RUNNING;
                runForMaster();
                LOG.info("It sounds like the previous master is gone, " +
                            "so let's run for master again."); 

                break;
            default:
                checkMaster();
                break;
            }
        }
    };

    Watcher masterExistsWatcher = new Watcher(){
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeDeleted) {
                assert znodePath.equals( e.getPath() );

                runForMaster();
            }
        }
    };

    void takeLeadership() {
        //LOG.info("Going for list of workers");
        //getWorkers();
    }

    /*
     * Run for master. To run for master, we try to create the /master znode,
     * with masteCreateCallback being the callback implementation. 
     * In the case the create call succeeds, the client becomes the master.
     * If it receives a CONNECTIONLOSS event, then it needs to check if the 
     * znode has been created. In the case the znode exists, it needs to check
     * which server is the master.
     */

    /**
     * Tries to create a /master lock znode to acquire leadership.
     */
    public void runForMaster() {
        LOG.info("Running for master");
        zk.create(znodePath, 
                serverId.getBytes(), 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.EPHEMERAL,
                masterCreateCallback,
                null);
    }

    DataCallback masterCheckCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                checkMaster();

                break;
            case NONODE:
                runForMaster();

                break; 
            case OK:
                if( serverId.equals( new String(data) ) ) {
                    state = MasterStates.ELECTED;
                    takeLeadership();
                } else {
                    state = MasterStates.NOTELECTED;
                    masterExists();
                }

                break;
            default:
                LOG.error("Error when reading data.", 
                        KeeperException.create(Code.get(rc), path));
            }
        } 
    };
        
    void checkMaster() {
        zk.getData(znodePath, false, masterCheckCallback, null);
    }

    /**
     * Closes the ZooKeeper session. 
     * 
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if(zk != null) {
            try{
                zk.close();
            } catch (InterruptedException e) {
                LOG.warn( "Interrupted while closing ZooKeeper session.", e );
            }
        }
    }

    public boolean isLeader() {
        return (state == MasterStates.ELECTED);
    }

    /**
     * Main method providing an example of how to run the master.
     * 
     * @param args
     * @throws Exception
     */
/*
    public static void main(String args[]) throws Exception {
        Master m = new Master(args[0]);
        m.startZK();

        while(!m.isConnected()){
            Thread.sleep(100);
        }

        m.runForMaster();

        while(!m.isExpired()){
            Thread.sleep(1000);
        }   

        m.stopZK();
    }
*/
}
