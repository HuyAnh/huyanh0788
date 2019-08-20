package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.RowMapper;
import topica.dw.etl.mozart.workflow.common.IOCommon;
import topica.dw.etl.mozart.workflow.common.exception.UnsupportedGenerateSqlException;
import topica.dw.etl.mozart.workflow.writer.DwWriterProviderFactory;
import topica.dw.etl.mozart.workflow.writer.SqlWriterProvider;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.function.Function;

/**
 * <p>
 * Abstract ETL step builder to build the chunk step includes setup
 * reader{link @JdbcCursorItemReader}, writer{link @JdbcBatchItemWriter}
 * </p>
 *
 * @author trungnt9
 */
public abstract class AbstractSqlGeneratorChunkStepBuilder<T, V> extends AbstractChunkStepBuilder<T, V>
        implements DwWriterProviderFactory {
    private final String SQL_DEFAULT_RESOURCE_PATH = "/etl/sql/";

    @Autowired
    @Qualifier("dwTargetProvider")
    private DwWriterProviderFactory.WriterProvider targetProvider;


    public AbstractSqlGeneratorChunkStepBuilder(String stepName) {
        super(stepName);
    }

    protected final String getSqlOnDefaultFilePath(String fileName) throws FileNotFoundException {
        String sqlResource = SQL_DEFAULT_RESOURCE_PATH.concat(fileName);
        try {
            return IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(sqlResource));
        } catch (IOException e) {
            throw new FileNotFoundException("File " + sqlResource + " not found");
        }
    }

    @Override
    protected final ItemReader<T> createItemReader() throws Exception {
        JdbcCursorItemReader<T> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(getStagingDs());
        reader.setSql(buildReadSql());
        reader.setRowMapper(buildRowMapper());
        reader.setQueryTimeout(queryTimeout());
        reader.afterPropertiesSet();
        return reader;
    }

    @Override
    protected final ItemWriter<V> createItemWriter() throws Exception {
        JdbcBatchItemWriter<V> itemWriter = new JdbcBatchItemWriter<V>();
        itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<V>());
        itemWriter.setDataSource(getWarehouseDs());
        itemWriter.setSql(buildWriteSql());
        itemWriter.afterPropertiesSet();
        return itemWriter;
    }

    /**
     * @return
     */
    protected abstract RowMapper<T> buildRowMapper();

    /**
     * Build the reading query for ItemReader
     *
     * @return
     * @throws FileNotFoundException
     */
    protected abstract String buildReadSql() throws Exception;

    /**
     * Build the writing query for ItemWriter
     *
     * @return
     */
    protected abstract String buildWriteSql() throws UnsupportedGenerateSqlException;

    public Function<Class<V>, SqlWriterProvider<V>> getSqlProviderFunc() {
        return buildWriterProvider(targetProvider);
    }

}
