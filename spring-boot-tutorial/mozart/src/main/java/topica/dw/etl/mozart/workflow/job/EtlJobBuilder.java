package topica.dw.etl.mozart.workflow.job;

import org.springframework.batch.core.Job;

/**
 * <p>
 * Builder for build the Spring Batch ETL job
 * </p>
 *
 * @param <T>
 * @param <V>
 * @author trungnt9
 */
// @Scope("step")
public interface EtlJobBuilder {

    /**
     * @return
     * @throws Exception
     */
    Job buildEtlJob() throws Exception;

    /**
     * Get Etl Job Name
     *
     * @return
     */
    String getJobName();

    /**
     * Query timeout base one second
     *
     * @return
     */
    default Integer queryTimeout() {
        return 10 * 60;
    }

}
