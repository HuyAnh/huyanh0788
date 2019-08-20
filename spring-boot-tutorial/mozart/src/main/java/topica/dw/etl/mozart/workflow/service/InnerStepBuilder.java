package topica.dw.etl.mozart.workflow.service;

import org.springframework.batch.core.Step;

public interface InnerStepBuilder {

    /**
     * Default item reader page size
     */
    int DEFAULT_PAGE_SIZE = 10000;

    /**
     * Default item reader page size
     */
    int DEFAULT_CHUNK_SIZE = 5000;

    /**
     * @param jobMapping
     * @return
     * @throws Exception
     */
    Step buildLoadingBatchStep(String taskId) throws Exception;
}
