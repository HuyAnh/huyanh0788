package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.sql.DataSource;

/**
 * @param <T>
 * @param <V>
 * @author trungnt9
 */
public abstract class AbstractChunkStepBuilder<T, V> implements ChunkStepBuilder<T, V> {
    @Autowired
    private StepBuilderFactory stepFactory;

    @Autowired
    @Qualifier("stagingDs")
    private DataSource stagingDs;

    @Autowired
    @Qualifier("warehouseDS")
    private DataSource warehouseDs;


    @Autowired
    @Qualifier("job_ds")
    private DataSource jobDs;

    private String stepName;

    public AbstractChunkStepBuilder(String stepName) {
        this.stepName = stepName;
    }

    @Override
    public final Step createStep() throws Exception {
        return stepFactory.get(getStepName()).<T, V>chunk(getChunkSize()).reader(createItemReader())
                .processor(createItemProcessor()).writer(createItemWriter()).listener(stepExecutionListener(jobDs)).allowStartIfComplete(true).build();
    }

    /**
     * @return
     * @throws Exception
     */
    protected abstract ItemReader<T> createItemReader() throws Exception;

    /**
     * @return
     * @throws Exception
     */
    protected abstract ItemWriter<V> createItemWriter() throws Exception;

    /**
     * @return
     */
    protected abstract ItemProcessor<T, V> createItemProcessor();

    /**
     * That method used to generate default size of chunk
     *
     * @return
     */
    protected int getChunkSize() {
        return DEFAULT_CHUNK_SIZE;
    }

    public StepBuilderFactory getStepFactory() {
        return stepFactory;
    }

    public String getStepName() {
        return stepName;
    }

    public DataSource getStagingDs() {
        return stagingDs;
    }

    public DataSource getWarehouseDs() {
        return warehouseDs;
    }

}
