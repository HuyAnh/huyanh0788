package topica.dw.etl.mozart.workflow.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import topica.dw.etl.mozart.workflow.config.ApplicationConfig;
import topica.dw.etl.mozart.workflow.event.TaskSubscriber;

@SpringBootApplication
public class ApplicationRunner {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationRunner.class);
    private final ApplicationContext appCtx;

    public ApplicationRunner() {
        appCtx = new AnnotationConfigApplicationContext(ApplicationConfig.class);
    }

    private void initTaskSubscriber() {
        Thread subscriber = new Thread(appCtx.getBean(TaskSubscriber.class));
        LOG.info("Starting Subscriber Task Thread");
        subscriber.start();
    }

    public static void main(String args[]) throws Exception {
        ApplicationRunner runner = new ApplicationRunner();
        runner.initTaskSubscriber();
    }

    public ApplicationContext getAppCtx() {
        return appCtx;
    }
}
