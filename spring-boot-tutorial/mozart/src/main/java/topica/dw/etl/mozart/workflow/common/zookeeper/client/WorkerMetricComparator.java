package topica.dw.etl.mozart.workflow.common.zookeeper.client;

import java.util.Comparator;

public class WorkerMetricComparator implements Comparator<WorkerRuntimeMetric> {

    enum PriorityCoefficient {
        MEMORY(5d), CPU(2d), EXECUTOR(1.2d), TASK_INQUEUE(5d), NUM_OF_PROCESSOR(0.8d);

        private Double coefficient;

        PriorityCoefficient(Double coefficient) {
            this.coefficient = coefficient;
        }

        public Double getCoefficient() {
            return coefficient;
        }

        public void setCoefficient(Double coefficient) {
            this.coefficient = coefficient;
        }

    }

    @Override
    public int compare(WorkerRuntimeMetric metric1, WorkerRuntimeMetric metric2) {
        Double score = 0.0;
        Long memDiff = metric1.freeMemory() - metric2.freeMemory();
        if (memDiff != 0) {
            if (memDiff > 0) {
                score += makeScoreOnPercentage((double) ((memDiff / metric2.freeMemory())) * 100,
                        PriorityCoefficient.MEMORY);
            } else {
                score -= makeScoreOnPercentage((double) ((Math.abs(memDiff) / metric1.freeMemory())) * 100,
                        PriorityCoefficient.MEMORY);
            }
        }
        Integer avaibleExecutorDiff = metric1.getAvaiableExecutor() - metric2.getAvaiableExecutor();
        if (avaibleExecutorDiff != 0) {
            if (avaibleExecutorDiff > 0) {
                if (metric2.getAvaiableExecutor() == 0) {
                    score += PriorityCoefficient.EXECUTOR.getCoefficient();
                } else {
                    score += PriorityCoefficient.EXECUTOR.getCoefficient() / 2;
                }
            } else {
                if (metric1.getAvaiableExecutor() == 0) {
                    score -= PriorityCoefficient.EXECUTOR.getCoefficient();
                } else {
                    score -= PriorityCoefficient.EXECUTOR.getCoefficient() / 2;
                }
            }
        }
        Integer inqueueTaskDiff = metric1.getInqueueTask() - metric2.getInqueueTask();
        if (inqueueTaskDiff != 0) {
            if (inqueueTaskDiff > 0) {
                if (inqueueTaskDiff / metric2.getNumOfExecutorTask() > 0.8) {
                    score -= PriorityCoefficient.TASK_INQUEUE.getCoefficient();
                } else {
                    score -= PriorityCoefficient.TASK_INQUEUE.getCoefficient() / 2;
                }
            } else {
                if (inqueueTaskDiff / metric2.getNumOfExecutorTask() > 0.8) {
                    score += PriorityCoefficient.TASK_INQUEUE.getCoefficient();
                } else {
                    score += PriorityCoefficient.TASK_INQUEUE.getCoefficient() / 2;
                }
            }
        }
        Integer numOfCpuDiff = metric1.getNumOfCpu() - metric2.getNumOfCpu();
        if (numOfCpuDiff != 0) {
            if (numOfCpuDiff > 0) {
                score += PriorityCoefficient.NUM_OF_PROCESSOR.getCoefficient();
            } else {
                score -= PriorityCoefficient.NUM_OF_PROCESSOR.getCoefficient();
            }
        }
        return score >= 0 ? 1 : -1;
    }

    private double makeScoreOnPercentage(Double percentage, PriorityCoefficient coefficient) {
        if (percentage < 0.1) {
            return coefficient.getCoefficient() / 10;
        } else if (percentage >= 0.1 && percentage < 0.3) {
            return coefficient.getCoefficient() / 5;
        } else if (percentage >= 0.3 && percentage < 0.6) {
            return coefficient.getCoefficient() / 3;
        } else if (percentage >= 0.6 && percentage < 0.8) {
            return coefficient.getCoefficient() / 2;
        } else if (percentage > 0.8) {
            return coefficient.getCoefficient();
        }
        return 0.0;
    }

}
