package topica.dw.etl.mozart.workflow.common.zookeeper;

public class RecreateTaskCtx {
    String path;
    String task;
    byte[] data;

    RecreateTaskCtx(String path, String task, byte[] data) {
        this.path = path;
        this.task = task;
        this.data = data;
    }
}
