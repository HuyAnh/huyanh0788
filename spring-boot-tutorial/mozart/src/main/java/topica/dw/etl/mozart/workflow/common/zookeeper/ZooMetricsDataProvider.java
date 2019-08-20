package topica.dw.etl.mozart.workflow.common.zookeeper;

import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import topica.dw.etl.mozart.workflow.common.javametrics.CPUDataProvider;
import topica.dw.etl.mozart.workflow.common.javametrics.EnvironmentDataProvider;
import topica.dw.etl.mozart.workflow.common.javametrics.MemoryPoolDataProvider;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The class is designed to populate JVM metrics and tasks executing and put to
 * ZNode The metrics will be used to help the master exactly choice the worker
 * to execute the ETL Task
 *
 * @author trungnt9
 */
public class ZooMetricsDataProvider implements Watcher, Closeable, DisposableBean {

    private ZooKeeper zk;
    private String hostPort;
    private String serverId;
    private final Integer timeout;
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    private static final Logger LOG = LoggerFactory.getLogger(ZooMetricsDataProvider.class);
    private static final int DEFAULT_INTERVAL = 5;
    private ScheduledExecutorService exec;
    private boolean started;
    private String workerPath;
    private String workerNode;
    private String cpuPath;
    private String sysinfoPath;
    private String memoryPath;
    private String taskPath;

    @Autowired
    @Qualifier("alohomora_etl_task_control")
    private AlohomoraTasksControl taskControl;

    public ZooMetricsDataProvider(String hostPort, Integer timeout, String serverId) throws Exception {
        this.hostPort = hostPort;
        this.timeout = timeout;
        this.serverId = serverId;
        initiateProcess();
    }

    private void initiateProcess() throws Exception {
        exec = Executors.newSingleThreadScheduledExecutor();
        startZK();
        if (!started) {
            buildZNodePath();
            createZNode();
        }
        populateMetrics(DEFAULT_INTERVAL);
        started = true;
    }

    private void buildZNodePath() {
        workerNode = "/workers";
        workerPath = workerNode.concat("/worker-" + serverId);
        cpuPath = workerPath.concat("/cpu");
        memoryPath = workerPath.concat("/memory");
        sysinfoPath = workerPath.concat("/sysinfo");
        taskPath = workerPath.concat("/tasks");
    }

    /**
     * Creates a ZooKeeper session.
     *
     * @throws IOException
     */
    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, timeout, this);
    }

    private void createZNode() throws Exception {
        createParent(workerNode, new byte[0]);
        createParent(workerPath, new byte[0]);
        zk.create(workerPath.concat("/status"), "running".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                createEphemeralCallback, null);
        // For CPU Node
        createParent(cpuPath, new byte[0]);
        zk.create(cpuPath.concat("/process"), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                createEphemeralCallback, null);
        zk.create(cpuPath.concat("/system"), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                createEphemeralCallback, null);
        // For Memory Node
        createParent(memoryPath, new byte[0]);
        zk.create(memoryPath.concat("/max"), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                createEphemeralCallback, null);
        zk.create(memoryPath.concat("/used"), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                createEphemeralCallback, null);
        zk.create(memoryPath.concat("/native"), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                createEphemeralCallback, null);
        zk.create(memoryPath.concat("/heap_after_gc"), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                createEphemeralCallback, null);
        // For Tasks Node
        createParent(sysinfoPath, new byte[0]);
        zk.create(sysinfoPath.concat("/os_architecture"), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                createEphemeralCallback, null);
        zk.create(sysinfoPath.concat("/num_of_cpu"), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                createEphemeralCallback, null);
        zk.create(sysinfoPath.concat("/hostname"), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                createEphemeralCallback, null);
        // For environment node
        createParent(taskPath, new byte[0]);
        zk.create(taskPath.concat("/running"), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                createEphemeralCallback, null);
        zk.create(taskPath.concat("/inqueue"), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                createEphemeralCallback, null);
        zk.create(taskPath.concat("/num_of_executor"), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                createEphemeralCallback, null);

    }

    private void populateMetrics(long interval) throws Exception {
        exec.scheduleAtFixedRate(this::emitCpuUsage, interval, interval, TimeUnit.SECONDS);
        exec.scheduleAtFixedRate(this::emitMemoryUsage, interval, interval, TimeUnit.SECONDS);
        exec.scheduleAtFixedRate(this::edmitTasks, interval, interval, TimeUnit.SECONDS);
        exec.scheduleAtFixedRate(this::emitEnvironmentData, interval, interval, TimeUnit.SECONDS);
    }

    /**
     *
     */
    private void emitCpuUsage() {
        if (!connected || expired)
            return;
        Integer version = -1;
        double process = CPUDataProvider.getProcessCpuLoad();
        double system = CPUDataProvider.getSystemCpuLoad();
        try {
            String processPath = cpuPath.concat("/process");
            if (zk.exists(processPath, false) != null)
                zk.setData(cpuPath.concat("/process"), String.valueOf(process).getBytes(), version);

            String systemPath = cpuPath.concat("/system");
            if (zk.exists(systemPath, false) != null)
                zk.setData(cpuPath.concat("/system"), String.valueOf(system).getBytes(), version);
        } catch (KeeperException | InterruptedException e) {
            LOG.error("Error when set data for cpu usage on znode", e);
        }
    }

    /**
     *
     */
    private void emitMemoryUsage() {
        if (!connected || expired)
            return;
        long liveHeap = MemoryPoolDataProvider.getHeapMemory();
        long usedNative = MemoryPoolDataProvider.getNativeMemory();
        long maxMemory = MemoryPoolDataProvider.getMaxMemory();
        long usedHeapAfterGC = MemoryPoolDataProvider.getUsedHeapAfterGC();
        int version = -1;
        try {
            String usedPath = memoryPath.concat("/used");
            if (zk.exists(usedPath, false) != null)
                zk.setData(usedPath, String.valueOf(liveHeap).getBytes(), version);

            String maxPath = memoryPath.concat("/max");
            if (zk.exists(maxPath, false) != null)
                zk.setData(maxPath, String.valueOf(maxMemory).getBytes(), version);

            String nativePath = memoryPath.concat("/native");
            if (zk.exists(nativePath, false) != null)
                zk.setData(nativePath, String.valueOf(usedNative).getBytes(), version);

            String heapAfterGcPath = memoryPath.concat("/heap_after_gc");
            if (zk.exists(heapAfterGcPath, false) != null)
                zk.setData(heapAfterGcPath, String.valueOf(usedHeapAfterGC).getBytes(), version);
        } catch (KeeperException | InterruptedException e) {
            LOG.error("Error when set data for heap on znode", e);
        }
    }

    /**
     *
     */
    private void edmitTasks() {
        if (!connected || expired)
            return;
        int version = -1;
        try {
            String runningPath = taskPath.concat("/running");
            if (zk.exists(runningPath, false) != null)
                zk.setData(runningPath, String.valueOf(taskControl.getRunningTasks()).getBytes(), version);

            String inqueuePath = taskPath.concat("/inqueue");
            if (zk.exists(inqueuePath, false) != null)
                zk.setData(inqueuePath, String.valueOf(taskControl.getTotalInQueueTasks()).getBytes(), version);

            String numOfExecutorPath = taskPath.concat("/num_of_executor");
            if (zk.exists(numOfExecutorPath, false) != null)
                zk.setData(numOfExecutorPath, String.valueOf(taskControl.getNumOfExecutor()).getBytes(), version);
        } catch (KeeperException | InterruptedException e) {
            LOG.error("Error when set data for heap on znode", e);
        }
    }

    /**
     *
     */
    private void emitEnvironmentData() {
        if (!connected || expired)
            return;
        int cpuCount = EnvironmentDataProvider.getCPUCount();
        try {
            String numOfCpuPath = sysinfoPath.concat("/num_of_cpu");
            if (zk.exists(numOfCpuPath, false) != null)
                zk.setData(numOfCpuPath, String.valueOf(cpuCount).getBytes(), -1);

            String osArchitecturePath = sysinfoPath.concat("/os_architecture");
            if (zk.exists(osArchitecturePath, false) != null)
                zk.setData(osArchitecturePath,
                        String.valueOf(escapeStringForJSON(EnvironmentDataProvider.getArchitecture())).getBytes(), -1);

            String hostnamePath = sysinfoPath.concat("/hostname");
            if (zk.exists(hostnamePath, false) != null)
                zk.setData(hostnamePath,
                        String.valueOf(escapeStringForJSON(EnvironmentDataProvider.getHostname())).getBytes(), -1);
        } catch (Exception e) {
            LOG.error("Error when set data for environment on znode", e);
        }
    }

    void createParent(String path, byte[] data) {
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, null);
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

    StringCallback createEphemeralCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    /*
                     * Try again. Note that registering again is not a problem. If
                     * the znode has already been created, then we get a NODEEXISTS
                     * event back.
                     */
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

    public void restartZK() throws Exception {
        this.close();
        initiateProcess();
    }

    @Override
    public void destroy() throws Exception {
        this.close();
    }

    @Override
    public void close() throws IOException {
        LOG.info("The Metrics Crawling is Closing ...");
        try {
            zk.close();
            exec.shutdown();
        } catch (InterruptedException e) {
            LOG.warn("ZooKeeper interrupted while closing");
        }
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
                    try {
                        restartZK();
                    } catch (Exception ex) {
                        LOG.error("Can't restart this service", ex);
                    }
                default:
                    break;
            }
        }
    }

    private static String escapeStringForJSON(String str) {
        return str.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
