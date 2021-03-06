package topica.dw.etl.mozart.workflow.writer.provider.mysql;

import topica.dw.etl.mozart.workflow.common.exception.UnsupportedGenerateSqlException;
import topica.dw.etl.mozart.workflow.writer.SqlWriterProvider;

import java.util.function.Function;

/**
 * @param <T>
 * @author trungnt9
 * Default interface that provides the build writer provider function of MySql Database
 */
public interface MySqlWriterProvider<T> {
    public default Function<Class<T>, SqlWriterProvider<T>> buildProvider() {
        return t -> {
            return new MySqlAnnotationPropertyWriterProvider<T>() {
                @Override
                public String writeSql() throws UnsupportedGenerateSqlException {
                    return extractSqlFromThis(t);
                }
            };
        };
    }
}
