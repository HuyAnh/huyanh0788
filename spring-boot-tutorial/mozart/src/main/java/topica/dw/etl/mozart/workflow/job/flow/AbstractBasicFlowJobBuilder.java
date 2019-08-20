package topica.dw.etl.mozart.workflow.job.flow;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import topica.dw.etl.mozart.workflow.job.EtlJobBuilder;

/**
 * That provides basic method to create flow job
 *
 * @author trungnt9
 */
public abstract class AbstractBasicFlowJobBuilder implements EtlJobBuilder {

    @Autowired
    private JobBuilderFactory jobFactory;

    @Autowired
    private StepBuilderFactory stepFactory;

    private String jobName;

    public AbstractBasicFlowJobBuilder(String jobName) {
        this.jobName = jobName;
    }

    @Override
    public Job buildEtlJob() throws Exception {
        return jobFactory.get(getJobName()).incrementer(new RunIdIncrementer()).start(buildFlow()).end().build();
    }

    /**
     * Abstract method to build flow framework
     *
     * @return
     */
    protected abstract Flow buildFlow() throws Exception;

    /**
     * @return
     */
    public JobBuilderFactory getJobFactory() {
        return jobFactory;
    }

    /**
     * @return
     */
    public StepBuilderFactory getStepFactory() {
        return stepFactory;
    }

    /**
     * @return
     */
    public String getJobName() {
        return jobName;
    }

}
