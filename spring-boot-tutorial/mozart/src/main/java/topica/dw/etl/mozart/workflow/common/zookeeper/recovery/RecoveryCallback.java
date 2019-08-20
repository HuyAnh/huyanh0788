package topica.dw.etl.mozart.workflow.common.zookeeper.recovery;

import java.util.List;


/**
 * Callback interface. Called once recovery completes or fails.
 */
public interface RecoveryCallback {
    final static int OK = 0;
    final static int FAILED = -1;

    public void recoveryComplete(int rc, List<String> tasks);
}
