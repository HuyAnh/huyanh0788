package topica.dw.etl.mozart.workflow.job.chunk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.RowMapper;
import topica.dw.etl.mozart.workflow.common.BeanCopyCommon;
import topica.dw.etl.mozart.workflow.common.exception.UnsupportedGenerateSqlException;

import java.lang.reflect.Constructor;

/**
 * Wrapper.
 */
public class SqlGeneratorChunkStepBuilder<T, V> extends AbstractSqlGeneratorChunkStepBuilder<T, V> {

    private static final Logger LOG = LoggerFactory.getLogger(DepartmentDimChunk.class);
    private Class<T> sourceType;
    private Class<V> targetType;

    public SqlGeneratorChunkStepBuilder(Class<T> sourceType, Class<V> targetType, String stepName) {
        super(stepName);
        this.sourceType = sourceType;
        this.targetType = targetType;
    }

    @Override
    protected RowMapper buildRowMapper() {
        return ((resultSet, i) -> {
            try {

                Class<?>[] parameters = new Class[]{};
                Constructor<T> constructor = sourceType.getConstructor(parameters);

                Object[] arguments = new Object[]{};
                T item = constructor.newInstance(arguments);
                BeanCopyCommon.mapFromResultSetToPojoObject().apply(resultSet, item);

                return item;
            } catch (Exception e) {
                LOG.error("Cannot convert item {} to object with error: {}", i, e);
                return null;
            }
        });
    }

    @Override
    protected String buildReadSql() throws Exception {
        return getSqlOnDefaultFilePath(getStepName() + ".sql");
    }

    @Override
    protected String buildWriteSql() throws UnsupportedGenerateSqlException {
        return getSqlProviderFunc().apply(targetType).writeSql();
    }

    @Override
    protected ItemProcessor<T, V> createItemProcessor() {
        return t -> (V) t;
    }
}
