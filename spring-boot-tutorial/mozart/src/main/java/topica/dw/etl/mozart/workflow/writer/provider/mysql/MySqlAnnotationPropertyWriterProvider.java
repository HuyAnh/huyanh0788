package topica.dw.etl.mozart.workflow.writer.provider.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import topica.dw.etl.mozart.workflow.common.ReflectionCommon;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.PrimaryKey;
import topica.dw.etl.mozart.workflow.common.exception.UnsupportedGenerateSqlException;
import topica.dw.etl.mozart.workflow.writer.SqlWriterProvider;

import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @param <T>
 * @author trungnt9
 */
public interface MySqlAnnotationPropertyWriterProvider<T> extends SqlWriterProvider<T> {

    final Logger LOG = LoggerFactory.getLogger(MySqlAnnotationPropertyWriterProvider.class);
    final String ON_UPDATE_STATEMENT = "ON DUPLICATE KEY UPDATE";

    /**
     * @return
     */
    default String extractSqlFromThis(Class<T> cls) throws UnsupportedGenerateSqlException {
        DWTable tableAnno = cls.getDeclaredAnnotation(DWTable.class);
        if (tableAnno != null) {
            String tableName = tableAnno.value();
            LOG.debug("Extract and generate sql for table:" + tableName + ", on class:" + cls);
            List<Field> listColumn = ReflectionCommon.getAllDeclaredFieldsOfAnnotation(cls, DWTableColumn.class);
            List<Field> listPk = ReflectionCommon.getAllDeclaredFieldsOfAnnotation(cls, PrimaryKey.class);
            // Populate all declared fields of class
            if (!listColumn.isEmpty()) {
                StringBuilder builder = new StringBuilder();
                builder.append(INSERT_QUERY.concat(tableName));
                builder.append(LEFT_PARENTHESIS);
                builder.append(listColumn.stream().map(e -> e.getAnnotation(DWTableColumn.class).value())
                        .collect(Collectors.joining(", ")));
                builder.append(RIGHT_PARENTHESIS).append(SPACE).append(VALUES).append(LEFT_PARENTHESIS);
                builder.append(
                        listColumn.stream().map(e -> COLON.concat(e.getName())).collect(Collectors.joining(", ")));
                builder.append(RIGHT_PARENTHESIS);
                if (!listPk.isEmpty()) {
                    builder.append(SPACE).append(ON_UPDATE_STATEMENT).append(SPACE);
                    builder.append(listColumn.stream().filter(e -> !listPk.contains(e))
                            .map(x -> (new StringBuilder(x.getAnnotation(DWTableColumn.class).value())).append(SPACE)
                                    .append(EQUAL).append(SPACE).append(COLON).append(x.getName()).toString())
                            .collect(Collectors.joining(", ")));
                }
                return builder.toString();
            }
        }
        throw new UnsupportedGenerateSqlException("Wrong configuration of class:" + cls);
    }
}
