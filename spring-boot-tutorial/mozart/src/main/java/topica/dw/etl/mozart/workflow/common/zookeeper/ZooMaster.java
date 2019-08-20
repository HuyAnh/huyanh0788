package topica.dw.etl.mozart.workflow.common.zookeeper;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.AsyncCallback.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import topica.dw.etl.mozart.workflow.common.zookeeper.client.WorkerMetricComparator;
import topica.dw.etl.mozart.workflow.common.zookeeper.client.WorkerRuntimeMetric;
import topica.dw.etl.mozart.workflow.common.zookeeper.client.WorkerRuntimeMetric.WorkerRuntimeMetricBuilder;
import topica.dw.etl.mozart.workflow.common.zookeeper.recovery.RecoveredAssignments;
import topica.dw.etl.mozart.workflow.common.zookeeper.recovery.RecoveryCallback;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class ZooMaster implements Watcher, Closeable, DisposableBean {
    private static final Logger LOG = LoggerFactory.getLogger(ZooMaster.class);

    enum MasterStates {
        RUNNING, ELECTED, NOTELECTED
    }

    ;

    private volatile MasterStates state = MasterStates.RUNNING;
    private Random random = new Random(this.hashCode());
    private ZooKeeper zk;
    private String hostPort;
    private String serverId;
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    protected ChildrenCache tasksCache;
    protected ChildrenCache workersCache;
    private Integer timeout;

    /**
     * Creates a new master instance.
     *
     * @param hostPort
     */
    public ZooMaster(String hostPort, Integer timeout, String serverId) {
        this.hostPort = hostPort;
        this.timeout = timeout;
        this.serverId = serverId;
    }

    @PostConstruct
    public void doInit() throws Exception {
        startZK();
        while (!isConnected()) {
            Thread.sleep(100);
        }
        /*
         * bootstrap() creates some necessary znodes.
         */
        bootstrap();
        /*
         * now runs for master.
         */
        runForMaster();
    }

    /**
     * Creates a new ZooKeeper session.
     *
     * @throws IOException
     */
    private void startZK() throws IOException {
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

    @Override
    public void close() throws IOException {
        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while closing ZooKeeper session.", e);
            }
        }
    }

    @Override
    public void destroy() throws Exception {
        this.close();
    }

    @Override
    public void process(WatchedEvent e) {
        LOG.info("Processing event: " + e.toString());
        if (e.getType() == Event.EventType.None) {
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
     * This method creates some parent znodes we need for this example. In the
     * case the master is restarted, then this method does not need to be
     * executed a second time.
     */
    public void bootstrap() {
        String assignRootPath = "/assign";
        Stat stat;
        try {
            stat = zk.exists(assignRootPath, true);
            if (stat == null)
                createParent(assignRootPath, new byte[0]);
        } catch (Exception e) {
            LOG.error("Exception when check and create assign path", e);
        }
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    StringCallback createParentCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    /*
                     * Try again. Note that registering again is not a problem. If
                     * the znode has already been created, then we get a NODEEXISTS
                     * event back.
                     */
                    createParent(path, (byte[]) ctx);
                    break;
                case OK:
                    LOG.info("Parent created");
                    break;
                case NODEEXISTS:
                    LOG.warn("Parent already registered: " + path);

                    break;
                default:
                    LOG.error("Something went wrong: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void createParent(String path, byte[] data) {
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
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

    void masterExists() {
        zk.exists("/master", masterExistsWatcher, masterExistsCallback, null);
    }

    Watcher masterExistsWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if (e.getType() == EventType.NodeDeleted) {
                assert "/master".equals(e.getPath());
                System.out.println("Node deleted");
                runForMaster();
            }
        }
    };

    public void runForMaster() {
        LOG.info("Running for master");
        zk.create("/master", serverId.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback,
                null);
    }

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
                    LOG.error("Something went wrong when running for master.", KeeperException.create(Code.get(rc), path));
            }
            LOG.info("I'm " + (state == MasterStates.ELECTED ? "" : "not ") + "the leader " + serverId);
        }
    };

    void checkMaster() {
        zk.getData("/master", false, masterCheckCallback, null);
    }

    void takeLeadership() {
        LOG.info("Going for list of workers");
        getWorkers();
        (new RecoveredAssignments(zk)).recover(new RecoveryCallback() {
            public void recoveryComplete(int rc, List<String> tasks) {
                if (rc == RecoveryCallback.FAILED) {
                    LOG.error("Recovery of assigned tasks failed.");
                } else {
                    LOG.info("Assigning recovered tasks");
                    getTasks();
                }
            }
        });
    }

    void getTasks() {
        zk.getChildren("/tasks", tasksChangeWatcher, tasksGetChildrenCallback, null);
    }

    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTasks();

                    break;
                case OK:
                    List<String> toProcess;
                    if (tasksCache == null) {
                        tasksCache = new ChildrenCache(children);
                        toProcess = children;
                    } else {
                        toProcess = tasksCache.addedAndSet(children);
                    }

                    if (toProcess != null) {
                        assignTasks(toProcess);
                    }

                    break;
                default:
                    LOG.error("getChildren failed.", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void assignTasks(List<String> tasks) {
        for (String task : tasks) {
            getTaskData(task);
        }
    }

    void getTaskData(String task) {
        zk.getData("/tasks/" + task, false, taskDataCallback, task);
    }

    DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTaskData((String) ctx);
                    break;
                case OK:
                    /*
                     * Choose worker base on calculate metrics algorithm.
                     */
                    String designatedWorker = extractDesignatedWorker();
                    if (!StringUtils.isEmpty(designatedWorker)) {
                        /*
                         * Assign task to chosen worker.
                         */
                        String assignmentPath = "/assign/" + designatedWorker + "/" + (String) ctx;
                        LOG.info("Assignment path: " + assignmentPath);
                        createAssignment(assignmentPath, data);
                    }
                    break;
                default:
                    LOG.error("Error when trying to get task data.", KeeperException.create(Code.get(rc), path));
            }
        }

        private String extractDesignatedWorker() {
            List<String> list = workersCache.getList();
            if (!list.isEmpty()) {

                List<WorkerRuntimeMetric> listMetrics = list.stream().map(this::populateMetricOfWorker)
                        .filter(e -> e.canRanking()).collect(Collectors.toList());
                // When we can populate the metric, need to sort and get first
                if (listMetrics.size() > 0) {
                    Collections.sort(listMetrics, new WorkerMetricComparator());
                    return listMetrics.iterator().next().getWorkerName();
                } else {
                    // Choose worker at random.
                    return list.get(random.nextInt(list.size()));
                }
            }
            return null;
        }

        private WorkerRuntimeMetric populateMetricOfWorker(String worker) {
            String statusPath = "/workers/worker-" + serverId + "/status";
            String cpuPath = "/workers/worker-" + serverId + "/cpu";
            String memoryPath = "/workers/worker-" + serverId + "/memory";
            String sysinfoPath = "/workers/worker-" + serverId + "/sysinfo";
            String taskPath = "/workers/worker-" + serverId + "/tasks";
            WorkerRuntimeMetricBuilder builder = new WorkerRuntimeMetric.WorkerRuntimeMetricBuilder();
            builder.workerName(worker);
            try {
                String status = new String(zk.getData(statusPath, false, null));
                builder.buildRunningStatus(
                        StringUtils.isNotBlank(status) && "running".equalsIgnoreCase(status) ? true : false);
            } catch (Exception e) {
                LOG.error("Can't get cpu process data of worker[" + worker + "]", e);
            }

            try {
                String processCpu = new String(zk.getData(cpuPath.concat("/process"), false, null));
                builder.processCpu(processCpu);
            } catch (Exception e) {
                LOG.error("Can't get cpu process data of worker[" + worker + "]", e);
            }

            try {
                String systemCpu = new String(zk.getData(cpuPath.concat("/system"), false, null));
                builder.systemCpu(systemCpu);
            } catch (Exception e) {
                LOG.error("Can't get cpu system data of worker[" + worker + "]", e);
            }

            try {
                String maxMemory = new String(zk.getData(memoryPath.concat("/max"), false, null));
                builder.maxMemory(maxMemory);
            } catch (Exception e) {
                LOG.error("Can't get max memory data of worker[" + worker + "]", e);
            }

            try {
                String usedMemory = new String(zk.getData(memoryPath.concat("/used"), false, null));
                builder.usedMemory(usedMemory);
            } catch (Exception e) {
                LOG.error("Can't get used memory data of worker[" + worker + "]", e);
            }

            try {
                String nativeMemory = new String(zk.getData(cpuPath.concat("/native"), false, null));
                builder.nativeMemory(nativeMemory);
            } catch (Exception e) {
                LOG.error("Can't get native memory data of worker[" + worker + "]", e);
            }

            try {
                String heapAfterGc = new String(zk.getData(cpuPath.concat("/heap_after_gc"), false, null));
                builder.heapAfterGc(heapAfterGc);
            } catch (Exception e) {
                LOG.error("Can't get heap after gc memory data of worker[" + worker + "]", e);
            }

            try {
                String osArchitecture = new String(zk.getData(sysinfoPath.concat("/os_architecture"), false, null));
                builder.osArchitecture(osArchitecture);
            } catch (Exception e) {
                LOG.error("Can't get os architecture of worker[" + worker + "]", e);
            }

            try {
                String numOfCpu = new String(zk.getData(sysinfoPath.concat("/num_of_cpu"), false, null));
                builder.numOfCpu(numOfCpu);
            } catch (Exception e) {
                LOG.error("Can't get number of cpu of worker[" + worker + "]", e);
            }

            try {
                String hostName = new String(zk.getData(sysinfoPath.concat("/hostname"), false, null));
                builder.hostName(hostName);
            } catch (Exception e) {
                LOG.error("Can't get number of cpu of worker[" + worker + "]", e);
            }

            try {
                String runningTask = new String(zk.getData(taskPath.concat("/running"), false, null));
                builder.runningTask(runningTask);
            } catch (Exception e) {
                LOG.error("Can't get the running task of worker[" + worker + "]", e);
            }

            try {
                String inqueueTask = new String(zk.getData(taskPath.concat("/inqueue"), false, null));
                builder.inqueueTask(inqueueTask);
            } catch (Exception e) {
                LOG.error("Can't get the inqueue task of worker[" + worker + "]", e);
            }

            try {
                String numOfExecutor = new String(zk.getData(taskPath.concat("/num_of_executor"), false, null));
                builder.numOfExecutorTask(numOfExecutor);
            } catch (Exception e) {
                LOG.error("Can't get the number of executor of worker[" + worker + "]", e);
            }
            return builder.build();
        }
    };

    void createAssignment(String path, byte[] data) {
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, assignTaskCallback, data);
    }

    StringCallback assignTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    createAssignment(path, (byte[]) ctx);
                    break;
                case OK:
                    LOG.info("Task assigned correctly: " + name);
                    deleteTask(name.substring(name.lastIndexOf("/") + 1));
                    break;
                case NODEEXISTS:
                    LOG.warn("Task already assigned");

                    break;
                default:
                    LOG.error("Error when trying to assign task.", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    /*
     * Once assigned, we delete the task from /tasks
     */
    void deleteTask(String name) {
        zk.delete("/tasks/" + name, -1, taskDeleteCallback, null);
    }

    VoidCallback taskDeleteCallback = new VoidCallback() {
        public void processResult(int rc, String path, Object ctx) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    deleteTask(path);
                    break;
                case OK:
                    LOG.info("Successfully deleted " + path);
                    break;
                case NONODE:
                    LOG.info("Task has been deleted already");
                    break;
                default:
                    LOG.error("Something went wrong here, " + KeeperException.create(Code.get(rc), path));
            }
        }
    };

    Watcher tasksChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if (e.getType() == EventType.NodeChildrenChanged) {
                assert "/tasks".equals(e.getPath());
                getTasks();
            }
        }
    };

    void getWorkers() {
        zk.getChildren("/workers", workersChangeWatcher, workersGetChildrenCallback, null);
    }

    ChildrenCallback workersGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getWorkers();
                    break;
                case OK:
                    LOG.info("Succesfully got a list of workers: " + children.size() + " workers");
                    reassignAndSet(children);
                    break;
                default:
                    LOG.error("getChildren failed", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void reassignAndSet(List<String> children) {
        List<String> toProcess;

        if (workersCache == null) {
            workersCache = new ChildrenCache(children);
            toProcess = null;
        } else {
            LOG.info("Removing and setting");
            toProcess = workersCache.removedAndSet(children);
        }

        if (toProcess != null) {
            for (String worker : toProcess) {
                getAbsentWorkerTasks(worker);
            }
        }
    }

    void getAbsentWorkerTasks(String worker) {
        zk.getChildren("/assign/" + worker, false, workerAssignmentCallback, null);
    }

    ChildrenCallback workerAssignmentCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getAbsentWorkerTasks(path);
                    break;
                case OK:
                    LOG.info("Succesfully got a list of assignments: " + children.size() + " tasks");

                    /*
                     * Reassign the tasks of the absent worker.
                     */

                    for (String task : children) {
                        getDataReassign(path + "/" + task, task);
                    }
                    break;
                default:
                    LOG.error("getChildren failed", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    /**
     * Get reassigned task data.
     *
     * @param path Path of assigned task
     * @param task Task name excluding the path prefix
     */
    void getDataReassign(String path, String task) {
        zk.getData(path, false, getDataReassignCallback, task);
    }

    /**
     * Get task data reassign callback.
     */
    DataCallback getDataReassignCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getDataReassign(path, (String) ctx);
                    break;
                case OK:
                    recreateTask(new RecreateTaskCtx(path, (String) ctx, data));
                    break;
                default:
                    LOG.error("Something went wrong when getting data ", KeeperException.create(Code.get(rc)));
            }
        }
    };

    /**
     * Recreate task znode in /tasks
     *
     * @param ctx Recreate text context
     */
    void recreateTask(RecreateTaskCtx ctx) {
        zk.create("/tasks/" + ctx.task, ctx.data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, recreateTaskCallback,
                ctx);
    }

    /**
     * Recreate znode callback
     */
    StringCallback recreateTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    recreateTask((RecreateTaskCtx) ctx);

                    break;
                case OK:
                    deleteAssignment(((RecreateTaskCtx) ctx).path);
                    break;
                case NODEEXISTS:
                    LOG.info("Node exists already, but if it hasn't been deleted, "
                            + "then it will eventually, so we keep trying: " + path);
                    recreateTask((RecreateTaskCtx) ctx);

                    break;
                default:
                    LOG.error("Something wwnt wrong when recreating task", KeeperException.create(Code.get(rc)));
            }
        }
    };

    /**
     * Delete assignment of absent worker
     *
     * @param path Path of znode to be deleted
     */
    void deleteAssignment(String path) {
        zk.delete(path, -1, taskDeletionCallback, null);
    }

    VoidCallback taskDeletionCallback = new VoidCallback() {
        public void processResult(int rc, String path, Object rtx) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    deleteAssignment(path);
                    break;
                case OK:
                    LOG.info("Task correctly deleted: " + path);
                    break;
                default:
                    LOG.error("Failed to delete task data" + KeeperException.create(Code.get(rc), path));
            }
        }
    };

    Watcher workersChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if (e.getType() == EventType.NodeChildrenChanged) {
                assert "/workers".equals(e.getPath());
                getWorkers();
            }
        }
    };

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
                    if (serverId.equals(new String(data))) {
                        state = MasterStates.ELECTED;
                        takeLeadership();
                    } else {
                        state = MasterStates.NOTELECTED;
                        masterExists();
                    }

                    break;
                default:
                    LOG.error("Error when reading data.", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    StatCallback masterExistsCallback = new StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    masterExists();
                    break;
                case OK:
                    break;
                case NONODE:
                    state = MasterStates.RUNNING;
                    runForMaster();
                    LOG.info("It sounds like the previous master is gone, " + "so let's run for master again.");

                    break;
                default:
                    checkMaster();
                    break;
            }
        }
    };

}
