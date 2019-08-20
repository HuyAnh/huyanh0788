package topica.dw.etl.mozart.workflow.common.zookeeper.client;

import org.apache.commons.lang3.StringUtils;

/**
 * The class to hold the worker metrics
 *
 * @author trungnt9
 */
public class WorkerRuntimeMetric {
    private String workerName;
    private String hostName;
    private Double systemCpu;
    private Double processCpu;
    private Long maxMemory;
    private Long usedMemory;
    private Long nativeMemory;
    private Long heapAfterGc;
    private String osArchitecture;
    private Integer numOfCpu;
    private Integer runningTask;
    private Integer inqueueTask;
    private Integer numOfExecutorTask;
    private boolean running;

    public WorkerRuntimeMetric(String workerName, String hostName, Double systemCpu, Double processCpu, Long maxMemory,
                               Long usedMemory, Long nativeMemory, Long heapAfterGc, String osArchitecture, Integer numOfCpu,
                               Integer runningTask, Integer inqueueTask, Integer numOfExecutorTask, boolean running) {
        this.workerName = workerName;
        this.hostName = hostName;
        this.systemCpu = systemCpu;
        this.processCpu = processCpu;
        this.maxMemory = maxMemory;
        this.usedMemory = usedMemory;
        this.nativeMemory = nativeMemory;
        this.heapAfterGc = heapAfterGc;
        this.osArchitecture = osArchitecture;
        this.numOfCpu = numOfCpu;
        this.runningTask = runningTask;
        this.inqueueTask = inqueueTask;
        this.numOfExecutorTask = numOfExecutorTask;
        this.running = running;
    }

    public Double getSystemCpu() {
        return systemCpu;
    }

    public void setSystemCpu(Double systemCpu) {
        this.systemCpu = systemCpu;
    }

    public Double getProcessCpu() {
        return processCpu;
    }

    public void setProcessCpu(Double processCpu) {
        this.processCpu = processCpu;
    }

    public String getOsArchitecture() {
        return osArchitecture;
    }

    public void setOsArchitecture(String osArchitecture) {
        this.osArchitecture = osArchitecture;
    }

    public Integer getNumOfCpu() {
        return numOfCpu;
    }

    public void setNumOfCpu(Integer numOfCpu) {
        this.numOfCpu = numOfCpu;
    }

    public Integer getRunningTask() {
        return runningTask;
    }

    public void setRunningTask(Integer runningTask) {
        this.runningTask = runningTask;
    }

    public Integer getInqueueTask() {
        return inqueueTask;
    }

    public void setInqueueTask(Integer inqueueTask) {
        this.inqueueTask = inqueueTask;
    }

    public Integer getNumOfExecutorTask() {
        return numOfExecutorTask;
    }

    public void setNumOfExecutorTask(Integer numOfExecutorTask) {
        this.numOfExecutorTask = numOfExecutorTask;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public Long getMaxMemory() {
        return maxMemory;
    }

    public void setMaxMemory(Long maxMemory) {
        this.maxMemory = maxMemory;
    }

    public Long getUsedMemory() {
        return usedMemory;
    }

    public void setUsedMemory(Long usedMemory) {
        this.usedMemory = usedMemory;
    }

    public Long getNativeMemory() {
        return nativeMemory;
    }

    public void setNativeMemory(Long nativeMemory) {
        this.nativeMemory = nativeMemory;
    }

    public Long getHeapAfterGc() {
        return heapAfterGc;
    }

    public void setHeapAfterGc(Long heapAfterGc) {
        this.heapAfterGc = heapAfterGc;
    }

    public String getWorkerName() {
        return workerName;
    }

    public void setWorkerName(String workerName) {
        this.workerName = workerName;
    }

    public boolean canRanking() {
        return running;
    }

    public Long freeMemory() {
        if (maxMemory != null && nativeMemory != null) {
            return maxMemory - nativeMemory;
        }
        if (maxMemory != null && usedMemory != null) {
            return maxMemory - usedMemory;
        }

        return 0l;
    }

    public Integer getAvaiableExecutor() {
        if (numOfExecutorTask != null && runningTask != null) {
            return numOfExecutorTask - runningTask;
        }
        return 0;
    }

    public static class WorkerRuntimeMetricBuilder {
        private String workerName;
        private String hostName;
        private Double systemCpu;
        private Double processCpu;
        private Long maxMemory;
        private Long usedMemory;
        private Long nativeMemory;
        private Long heapAfterGc;
        private String osArchitecture;
        private Integer numOfCpu;
        private Integer runningTask;
        private Integer inqueueTask;
        private Integer numOfExecutorTask;
        private boolean running;

        public WorkerRuntimeMetricBuilder workerName(String workerName) {
            this.workerName = workerName;
            return this;
        }

        public WorkerRuntimeMetricBuilder hostName(String hostName) {
            this.hostName = hostName;
            return this;
        }

        public WorkerRuntimeMetricBuilder systemCpu(String systemCpu) {
            if (!StringUtils.isEmpty(systemCpu)) {
                this.systemCpu = Double.valueOf(systemCpu);
            }
            return this;
        }

        public WorkerRuntimeMetricBuilder processCpu(String processCpu) {
            if (!StringUtils.isEmpty(processCpu)) {
                this.processCpu = Double.valueOf(processCpu);
            }
            return this;
        }

        public WorkerRuntimeMetricBuilder maxMemory(String maxMemory) {
            if (!StringUtils.isEmpty(maxMemory)) {
                this.maxMemory = Long.valueOf(maxMemory);
            }
            return this;
        }

        public WorkerRuntimeMetricBuilder usedMemory(String usedMemory) {
            if (!StringUtils.isEmpty(usedMemory)) {
                this.usedMemory = Long.valueOf(usedMemory);
            }
            return this;
        }

        public WorkerRuntimeMetricBuilder heapAfterGc(String heapAfterGc) {
            if (!StringUtils.isEmpty(heapAfterGc)) {
                this.heapAfterGc = Long.valueOf(heapAfterGc);
            }
            return this;
        }

        public WorkerRuntimeMetricBuilder nativeMemory(String nativeMemory) {
            if (!StringUtils.isEmpty(nativeMemory)) {
                this.nativeMemory = Long.valueOf(nativeMemory);
            }
            return this;
        }

        public WorkerRuntimeMetricBuilder osArchitecture(String os) {
            this.osArchitecture = os;
            return this;
        }

        public WorkerRuntimeMetricBuilder numOfCpu(String numOfCpu) {
            if (!StringUtils.isEmpty(numOfCpu)) {
                this.numOfCpu = Integer.valueOf(numOfCpu);
            }
            return this;
        }

        public WorkerRuntimeMetricBuilder runningTask(String runningTask) {
            if (!StringUtils.isEmpty(runningTask)) {
                this.runningTask = Integer.valueOf(runningTask);
            }
            return this;
        }

        public WorkerRuntimeMetricBuilder inqueueTask(String inqueueTask) {
            if (!StringUtils.isEmpty(inqueueTask)) {
                this.inqueueTask = Integer.valueOf(inqueueTask);
            }
            return this;
        }

        public WorkerRuntimeMetricBuilder numOfExecutorTask(String numOfExecutorTask) {
            if (!StringUtils.isEmpty(numOfExecutorTask)) {
                this.numOfExecutorTask = Integer.valueOf(numOfExecutorTask);
            }
            return this;
        }

        public WorkerRuntimeMetricBuilder buildRunningStatus(boolean status) {
            this.running = status;
            return this;
        }

        public WorkerRuntimeMetric build() {
            return new WorkerRuntimeMetric(workerName, hostName, systemCpu, processCpu, maxMemory, usedMemory,
                    nativeMemory, heapAfterGc, osArchitecture, numOfCpu, runningTask, inqueueTask, numOfExecutorTask,
                    running);
        }
    }

    @Override
    public String toString() {
        return "WorkerRuntimeMetric [workerName=" + workerName + ", hostName=" + hostName + ", systemCpu=" + systemCpu
                + ", processCpu=" + processCpu + ", maxMemory=" + maxMemory + ", usedMemory=" + usedMemory
                + ", nativeMemory=" + nativeMemory + ", heapAfterGc=" + heapAfterGc + ", osArchitecture="
                + osArchitecture + ", numOfCpu=" + numOfCpu + ", runningTask=" + runningTask + ", inqueueTask="
                + inqueueTask + ", numOfExecutorTask=" + numOfExecutorTask + "]";
    }

}
