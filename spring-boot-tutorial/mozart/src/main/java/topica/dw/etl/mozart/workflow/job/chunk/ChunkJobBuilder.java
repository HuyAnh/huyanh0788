package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import topica.dw.etl.mozart.workflow.job.EtlJobBuilder;

/**
 * <p>
 * To build chunk job via parameters jobName, step builder and job builder factory
 * </p>
 *
 * @author trungnt9
 */
public class ChunkJobBuilder implements EtlJobBuilder {

    private final String jobName;
    private final ChunkStepBuilder<?, ?> stepBuilder;
    private final JobBuilderFactory jobFactory;

    public ChunkJobBuilder(String jobName, JobBuilderFactory jobFactory, ChunkStepBuilder<?, ?> stepBuilder) {
        this.jobName = jobName;
        this.stepBuilder = stepBuilder;
        this.jobFactory = jobFactory;
    }

    @Override
    public Job buildEtlJob() throws Exception {
        return jobFactory.get(getJobName()).start(stepBuilder.createStep()).build();
    }

    public String getJobName() {
        return jobName;
    }

    public ChunkStepBuilder<?, ?> getStepBuilder() {
        return stepBuilder;
    }

    public JobBuilderFactory getJobFactory() {
        return jobFactory;
    }

}
