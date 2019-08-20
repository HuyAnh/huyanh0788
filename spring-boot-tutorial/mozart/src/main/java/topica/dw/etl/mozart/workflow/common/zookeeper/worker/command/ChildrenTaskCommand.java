package topica.dw.etl.mozart.workflow.common.zookeeper.worker.command;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ChildrenTaskCommand implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ChildrenTaskCommand.class);
    private List<String> children;
    private DataCallback cb;
    private final String serverId;
    private final ZooKeeper zk;

    public ChildrenTaskCommand(String serverId, ZooKeeper zk) {
        this.serverId = serverId;
        this.zk = zk;
    }

    /*
     * Initializes input of anonymous class
     */
    public Runnable init(List<String> children, DataCallback cb) {
        this.children = children;
        this.cb = cb;
        return this;
    }

    @Override
    public void run() {
        if (children == null) {
            return;
        }
        for (String task : children) {
            LOG.trace("New task: {}", task);
            zk.getData("/assign/worker-" + serverId + "/" + task, false, cb, task);
        }

    }

}
