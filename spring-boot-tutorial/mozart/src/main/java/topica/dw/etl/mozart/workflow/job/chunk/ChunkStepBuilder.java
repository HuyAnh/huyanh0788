package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.batch.core.Step;
import org.springframework.context.annotation.Scope;
import topica.dw.etl.mozart.workflow.listener.StepExecutionNotificationListener;

import javax.sql.DataSource;

/**
 * <p>
 * To define how to build chunk step
 * </p>
 *
 * @param <T>
 * @param <V>
 * @author trungnt9
 */
@Scope("step")
public interface ChunkStepBuilder<T, V> {

    int DEFAULT_CHUNK_SIZE = 50000;

    /**
     * Make chunk step for ETL job
     *
     * @return
     * @throws Exception
     */
    Step createStep() throws Exception;

    /**
     * Get chunk step name
     *
     * @return
     */
    String getStepName();

    /**
     * Provides default method to listen step execute
     *
     * @return
     */
    public default StepExecutionNotificationListener stepExecutionListener(DataSource ds) {
        return new StepExecutionNotificationListener(getStepName(), ds);
    }

    /**
     * Query timeout base second
     *
     * @return
     */
    default Integer queryTimeout() {
        return 60 * 60;
    }
}
