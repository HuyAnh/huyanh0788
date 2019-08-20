package topica.dw.etl.mozart.workflow.common.zookeeper;

import org.apache.zookeeper.AsyncCallback.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import topica.dw.etl.mozart.workflow.common.zookeeper.client.EtlTask;
import topica.dw.etl.mozart.workflow.common.zookeeper.worker.command.ChildrenTaskCommand;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ZooWorker implements Watcher, Closeable, DisposableBean {
    private static final Logger LOG = LoggerFactory.getLogger(ZooWorker.class);
    private ZooKeeper zk;
    private String hostPort;
    private String serverId;
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    private String status;
    private Executor executor;
    private final Lock lock = new ReentrantLock();
    private final Integer timeout;
    protected ChildrenCache assignedTasksCache = new ChildrenCache();
    @Autowired
    @Qualifier("alohomora_etl_task_control")
    private AlohomoraTasksControl taskControl;

    public ZooWorker(String hostPort, Integer timeout, Executor executor, String serverId) {
        this.hostPort = hostPort;
        this.timeout = timeout;
        this.executor = executor;
        this.serverId = serverId;
    }

    @Override
    public void close() throws IOException {
        LOG.info("Worker is Closing ...");
        try {
            zk.close();
        } catch (InterruptedException e) {
            LOG.warn("ZooKeeper interrupted while closing");
        }
    }

    @PostConstruct
    public void doInit() throws Exception {
        startZK();
        while (!isConnected()) {
            Thread.sleep(500);
        }
        /*
         * bootstrap() create some necessary znodes.
         */
        bootstrap();
        /*
         * Registers this worker so that the leader knows that it is here.
         */
        register();

        /*
         * Getting assigned tasks.
         */
        getTasks();
    }

    @Override
    public void process(WatchedEvent e) {
        LOG.info(e.toString() + ", " + hostPort);
        if (e.getType() == Event.EventType.None) {
            switch (e.getState()) {
                case SyncConnected:
                    /*
                     * Registered with ZooKeeper
                     */
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    LOG.error("Session expired");
                default:
                    break;
            }
        }
    }

    @Override
    public void destroy() throws Exception {
        this.close();
    }

    /**
     * Creates a ZooKeeper session.
     *
     * @throws IOException
     */
    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, timeout, this);
    }

    /**
     * Checks if this client is connected.
     *
     * @return boolean
     */
    public boolean isConnected() {
        return connected;
    }

    /**
     * Checks if ZooKeeper session is expired.
     *
     * @return
     */
    public boolean isExpired() {
        return expired;
    }

    /**
     * Bootstrapping here is just creating a /assign parent znode to hold the
     * tasks assigned to this worker.
     */
    public void bootstrap() throws Exception {
        createAssignNode();
    }

    void createAssignNode() {
        try {
            String assignPath = "/assign";
            Stat stat = zk.exists(assignPath, false);
            if (stat == null) {
                zk.create(assignPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createAssignCallback,
                        null);
            }
            // Check assign path again
            stat = zk.exists(assignPath, false);
            if (stat != null)
                zk.create("/assign/worker-" + serverId, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                        createAssignCallback, null);
            else
                LOG.error("Can't create worker node on server: {}", serverId);
        } catch (Exception e) {
            LOG.error("Cant create assign node of server id: " + serverId, e);
        }
    }

    StringCallback createAssignCallback = (int rc, String path, Object ctx, String name) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                createAssignNode();
                break;
            case OK:
                LOG.info("Assign node created");
                break;
            case NODEEXISTS:
                LOG.warn("Assign node already registered");
                break;
            default:
                LOG.error("Something went wrong: " + KeeperException.create(Code.get(rc), path));
        }
    };

    String name;

    /**
     * Registering the new worker, which consists of adding a worker znode to
     * /workers.
     */
    public void register() {
        zk.create("/workers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createWorkerCallback, null);
        name = "worker-" + serverId;
        zk.create("/workers/" + name, "Idle".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                createWorkerCallback, null);
    }

    StringCallback createWorkerCallback = (int rc, String path, Object ctx, String name) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                register();
                break;
            case OK:
                LOG.info("Registered successfully: " + serverId);
                break;
            case NODEEXISTS:
                LOG.warn("Already registered: " + serverId);
                break;
            default:
                LOG.error("Something went wrong: ", KeeperException.create(Code.get(rc), path));
        }
    };

    StatCallback statusUpdateCallback = (int rc, String path, Object ctx, Stat stat) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                updateStatus((String) ctx);
                return;
            default:
                break;
        }
    };

    private void updateStatus(String status) {
        try {
            lock.lock();
            if (status == this.status) {
                zk.setData("/workers/" + name, status.getBytes(), -1, statusUpdateCallback, status);
            }
        } finally {
            lock.unlock();
        }
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    private int executionCount;

    void changeExecutionCount(int countChange) {
        try {
            lock.lock();
            executionCount += countChange;
            if (executionCount == 0 && countChange < 0) {
                // we have just become idle
                setStatus("Idle");
            }
            if (executionCount == 1 && countChange > 0) {
                // we have just become idle
                setStatus("Working");
            }
        } finally {
            lock.unlock();
        }
    }

    Watcher newTaskWatcher = (WatchedEvent e) -> {
        if (e.getType() == EventType.NodeChildrenChanged) {
            assert new String("/assign/worker-" + serverId).equals(e.getPath());
            getTasks();
        }

    };

    public void getTasks() {
        zk.getChildren("/assign/worker-" + serverId, newTaskWatcher, tasksGetChildrenCallback, null);
    }

    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    if (children != null) {
                        executor.execute((new ChildrenTaskCommand(serverId, zk)).init(children, taskDataCallback));
                    }
                    break;
                default:
                    LOG.error("getChildren failed: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };

    DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.getData(path, false, taskDataCallback, null);
                    break;
                case OK:
                    /*
                     * Executing a task in this example is simply printing out some
                     * string representing the task.
                     */
                    if (data != null) {
                        EtlTask task = new EtlTask(new String(data), taskStatusCreateCallback, taskVoidCallback, ctx, zk);
                        try {
                            taskControl.addToQueue(task);
                        } catch (Exception e) {
                            LOG.error("Add task to queue have a problem", e);
                        }
                    }
                    break;
                default:
                    LOG.error("Failed to get task data: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    StringCallback taskStatusCreateCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.create(path + "/status", "done".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                            taskStatusCreateCallback, null);
                    break;
                case OK:
                    LOG.info("Created status znode correctly: " + name);
                    break;
                case NODEEXISTS:
                    LOG.warn("Node exists: " + path);
                    break;
                default:
                    LOG.error("Failed to create task data: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    VoidCallback taskVoidCallback = new VoidCallback() {
        public void processResult(int rc, String path, Object rtx) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    break;
                case OK:
                    LOG.info("Task correctly deleted: " + path);
                    break;
                default:
                    LOG.error("Failed to delete task data" + KeeperException.create(Code.get(rc), path));
            }
        }
    };

}
