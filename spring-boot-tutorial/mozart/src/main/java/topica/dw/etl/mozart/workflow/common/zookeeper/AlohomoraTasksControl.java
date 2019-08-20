package topica.dw.etl.mozart.workflow.common.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import topica.dw.etl.mozart.workflow.common.zookeeper.client.EtlTask;
import topica.dw.etl.mozart.workflow.service.CommonEtlJobBuilderService;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * To control task executor, populate tasks metric to help make the
 * decider(master) can allocate tasks to worker
 *
 * @author trungnt9
 */
public class AlohomoraTasksControl {
    private static final Logger LOG = LoggerFactory.getLogger(AlohomoraTasksControl.class);
    private final String serverId;
    @Autowired
    private JobLauncher jobLauncher;
    private final Integer numOfExecutor;
    private Set<EtlTask> runnings = new HashSet<>();
    private BlockingQueue<EtlTask> queue = new LinkedBlockingQueue<>();
    private volatile boolean isStop;
    private Object lock = new Object();
    private ExecutorService executor;
    private List<Thread> listRunningThread = new ArrayList<>();

    public AlohomoraTasksControl(String serverId, Integer numOfExecutor) {
        this.serverId = serverId;
        this.numOfExecutor = numOfExecutor;
        executor = Executors.newFixedThreadPool(numOfExecutor);
    }

    @PostConstruct
    private void start() {
        for (int i = 0; i < numOfExecutor; i++) {
            Thread t = new Thread(new AguamentiCommand());
            listRunningThread.add(t);
            executor.submit(t);
        }
    }

    @PreDestroy
    private void detroy() {
        setStop(true);
        for (Thread t : listRunningThread) {
            t.interrupt();
        }
        listRunningThread.clear();
        executor.shutdown();
    }

    /**
     * Get running task
     *
     * @return : List running task
     */
    public Set<EtlTask> getRunnings() {
        return runnings;
    }

    public void setRunnings(Set<EtlTask> runnings) {
        this.runnings = runnings;
    }

    public String getServerId() {
        return serverId;
    }

    /**
     * Add task to queue
     *
     * @param task
     * @throws Exception
     */
    public void addToQueue(EtlTask task) throws Exception {
        if (!queue.contains(task) && !runnings.contains(task)) {
            LOG.info("Put task:" + task.toString() + " to queue");
            queue.put(task);
        }
    }

    /**
     * Calculate total tasks in queue
     *
     * @return
     */
    public Integer getTotalInQueueTasks() {
        return queue.size();
    }

    /**
     * Get the current running tasks
     *
     * @return
     */
    public Integer getRunningTasks() {
        return runnings.size();
    }

    private void addToRunnings(EtlTask task) {
        synchronized (lock) {
            runnings.add(task);
        }
    }

    private void removeRunningsTask(EtlTask task) {
        synchronized (lock) {
            runnings.remove(task);
        }
    }

    /**
     * The class to execute the ETL Task
     */
    class AguamentiCommand implements Runnable {
        final static String DATE_PATTERN = "";

        @Override
        public void run() {
            while (!isStop) {
                try {
                    EtlTask task = queue.take();
                    // Spring batch is not allowed running multiple tasks in
                    // same time
                    if (task != null) {
                        if (!containsEtlRunningTask(runnings, task.getEtlJobName())) {
                            addToRunnings(task);
                            ZooKeeper zk = task.getZk();
                            try {
                                LOG.info("Executing your task: " + task + " On Server Id:" + serverId);
                                Map<String, JobParameter> parameters = new HashMap<String, JobParameter>();

                                JobExecution execution = jobLauncher.run(
                                        CommonEtlJobBuilderService.lookupJob(task.getEtlJobName()),
                                        new JobParameters(parameters));
                                LOG.info("Finish execute task:" + task + " with exit code:" + execution.getExitStatus().getExitCode());
                                zk.create("/status/" + (String) task.getCtx(),
                                        buildSuccessMessage(execution).getBytes(), Ids.OPEN_ACL_UNSAFE,
                                        CreateMode.PERSISTENT, task.getCreateCallback(), null);
                            } catch (Exception e) {
                                LOG.error("Execute task " + task + " error", e);
                                zk.create("/status/" + (String) task.getCtx(),
                                        (serverId + "-exception:" + e.getMessage()).getBytes(), Ids.OPEN_ACL_UNSAFE,
                                        CreateMode.PERSISTENT, task.getCreateCallback(), null);
                            } finally {
                                removeRunningsTask(task);
                                zk.delete("/assign/worker-" + serverId + "/" + (String) task.getCtx(), -1,
                                        task.getDeleteCallback(), null);
                            }
                        } else {
                            // Because the task is running, we need to enqueue
                            // task again
                            queue.put(task);
                        }
                    }
                } catch (InterruptedException e) {
                    LOG.error("Interrupted exception", e);
                }
            }
        }

        /**
         * @param list
         * @param name
         * @return
         */
        private boolean containsEtlRunningTask(final Set<EtlTask> list, final String name) {
            return list.stream().filter(o -> o.getEtlJobName().equals(name)).findFirst().isPresent();
        }

        private String buildSuccessMessage(JobExecution execution) {
            StringBuilder builder = new StringBuilder("{\"status:\": \"done\", \"start_time\":\"");
            final DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
            builder.append(formatter.format(execution.getStartTime()));
            builder.append("\",");
            builder.append("\"end_time\":\"");
            builder.append(formatter.format(execution.getEndTime())).append("\"");

            Collection<StepExecution> collStepExecution = execution.getStepExecutions();
            if (!CollectionUtils.isEmpty(collStepExecution)) {
                Integer readCount = collStepExecution.stream().mapToInt(e -> e.getReadCount()).sum();
                builder.append(",\"read_count\":").append(readCount);
                Integer commitCount = collStepExecution.stream().mapToInt(e -> e.getCommitCount()).sum();
                builder.append(",\"commit_count\":").append(commitCount);
                Integer skipCount = collStepExecution.stream().mapToInt(e -> e.getSkipCount()).sum();
                builder.append(",\"skip_count\":").append(skipCount);
                Integer writeCount = collStepExecution.stream().mapToInt(e -> e.getWriteCount()).sum();
                builder.append(",\"write_count\":").append(writeCount);
            }
            builder.append(",\"all_failure\":").append(execution.getAllFailureExceptions().size());
            builder.append(",\"exit_code\":").append(execution.getExitStatus().getExitCode());

            return builder.append("}").toString();
        }
    }

    public void setStop(boolean isStop) {
        this.isStop = isStop;
    }

    public Integer getNumOfExecutor() {
        return numOfExecutor;
    }

}
