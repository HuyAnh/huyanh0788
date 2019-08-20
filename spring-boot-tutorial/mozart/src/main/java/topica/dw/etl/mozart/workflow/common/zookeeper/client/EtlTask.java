package topica.dw.etl.mozart.workflow.common.zookeeper.client;

import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.ZooKeeper;

import java.util.Date;
import java.util.Optional;

public class EtlTask {
    private String etlJobName;
    private StringCallback createCallback;
    private VoidCallback deleteCallback;
    private Object ctx;
    private Date timeAddToQueue = new Date();
    private Date startTime;
    private Date endTime;
    private ZooKeeper zk;

    public EtlTask(String etlJobName, StringCallback createCallback, VoidCallback deleteCallback, Object ctx,
                   ZooKeeper zk) {
        this.etlJobName = etlJobName;
        this.createCallback = createCallback;
        this.deleteCallback = deleteCallback;
        this.ctx = ctx;
        this.zk = zk;
    }

    public String getEtlJobName() {
        return etlJobName;
    }

    public void setEtlJobName(String etlJobName) {
        this.etlJobName = etlJobName;
    }

    public StringCallback getCreateCallback() {
        return createCallback;
    }

    public void setCreateCallback(StringCallback createCallback) {
        this.createCallback = createCallback;
    }

    public VoidCallback getDeleteCallback() {
        return deleteCallback;
    }

    public void setDeleteCallback(VoidCallback deleteCallback) {
        this.deleteCallback = deleteCallback;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public Object getCtx() {
        return ctx;
    }

    public void setCtx(Object ctx) {
        this.ctx = ctx;
    }

    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public Date getTimeAddToQueue() {
        return timeAddToQueue;
    }

    public void setTimeAddToQueue(Date timeAddToQueue) {
        this.timeAddToQueue = timeAddToQueue;
    }

    @Override
    public int hashCode() {
        return Optional.ofNullable(ctx).map(code -> ctx.hashCode()).orElseGet(() -> 0);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof EtlTask)) {
            return false;
        }
        EtlTask task = (EtlTask) o;
        return task.getCtx().equals(ctx);
    }

    @Override
    public String toString() {
        return etlJobName + "|" + ctx.toString();
    }
}
