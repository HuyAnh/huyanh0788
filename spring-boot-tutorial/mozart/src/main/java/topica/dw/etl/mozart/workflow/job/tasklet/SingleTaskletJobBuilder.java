package topica.dw.etl.mozart.workflow.job.tasklet;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import topica.dw.etl.mozart.workflow.job.EtlJobBuilder;

public class SingleTaskletJobBuilder implements EtlJobBuilder {

    private final JobBuilderFactory jobFactory;
    private final StepBuilderFactory stepFactory;
    private final Tasklet tasklet;
    private final String jobName;

    public SingleTaskletJobBuilder(JobBuilderFactory jobFactory, StepBuilderFactory stepFactory, String jobName,
                                   Tasklet tasklet) {
        this.jobFactory = jobFactory;
        this.stepFactory = stepFactory;
        this.jobName = jobName;
        this.tasklet = tasklet;
    }

    @Override
    public Job buildEtlJob() {
        Step cleanTempStep = stepFactory.get("Single step").allowStartIfComplete(true).tasklet(tasklet).build();
        return jobFactory.get(jobName).start(cleanTempStep).build();
    }

    @Override
    public String getJobName() {
        return jobName;
    }

}