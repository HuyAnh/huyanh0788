package topica.dw.etl.mozart.workflow.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.main.ApplicationRunner;

@Component
public class TaskSubscriber implements DisposableBean, Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationRunner.class);
    private Thread thread;
    private volatile boolean isStop;

    TaskSubscriber() {
        this.thread = new Thread(this);
    }

    @Override
    public void run() {
        while (!isStop) {
            doHangup();
        }
    }

    private void doHangup() {
        try {
            Thread.sleep(60 * 60 * 1000);
            LOG.info("The server still running for wait task submited");
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }

    }

    @Override
    public void destroy() throws Exception {
        isStop = true;

    }

    public Thread getThread() {
        return thread;
    }

}
