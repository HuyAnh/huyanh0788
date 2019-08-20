package topica.dw.etl.mozart.workflow.common;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.TransformFromAttribute;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.*;
import java.util.List;
import java.util.function.BiFunction;

public class BeanCopyCommon {
    private static final Logger LOG = LoggerFactory.getLogger(BeanCopyCommon.class);

    public static <T> BiFunction<ResultSet, T, Void> mapFromResultSetToPojoObject() {
        return (rs, pojo) -> {
            Class<?> pojoCls = pojo.getClass();
            List<Field> listColumn = ReflectionCommon.getAllDeclaredFieldsOfAnnotation(pojoCls, DWTableColumn.class);
            for (Field f : listColumn) {
                try {
                    f.setAccessible(true);
                    Class<?> type = f.getType();
                    // Firstly check the declared value of
                    // TransformFromAttribute class
                    String columnName = f.getDeclaredAnnotation(TransformFromAttribute.class) != null
                            ? f.getDeclaredAnnotation(TransformFromAttribute.class).value() : null;
                    boolean ignore = false;
                    if (StringUtils.isEmpty(columnName)) {
                        DWTableColumn columnMeta = f.getDeclaredAnnotation(DWTableColumn.class);
                        columnName = columnMeta.value();
                        ignore = columnMeta.ignoreAssign();
                    }
                    if (!ignore) {
                        if (!StringUtils.isEmpty(columnName)) {
                            if (type == String.class) {
                                String value = rs.getString(columnName);
                                if (value != null)
                                    f.set(pojo, value);
                            } else if (type == Integer.class) {
                                Integer value = rs.getInt(columnName);
                                if (value != null)
                                    f.set(pojo, value);
                            } else if (type == Boolean.class) {
                                Boolean value = rs.getBoolean(columnName);
                                if (value != null)
                                    f.set(pojo, value);
                            } else if (type == Double.class) {
                                Double value = rs.getDouble(columnName);
                                if (value != null)
                                    f.set(pojo, value);
                            } else if (type == Long.class) {
                                Long value = rs.getLong(columnName);
                                if (value != null)
                                    f.set(pojo, value);
                            } else if (type == Timestamp.class) {
                                Timestamp value = rs.getTimestamp(columnName);
                                if (value != null)
                                    f.set(pojo, value);
                            } else if (type == Date.class) {
                                Date value = rs.getDate(columnName);
                                if (value != null)
                                    f.set(pojo, value);
                            } else if (type == java.util.Date.class) {
                                Date value = rs.getDate(columnName);
                                if (value != null)
                                    f.set(pojo, new java.util.Date(value.getTime()));
                            } else if (type == BigDecimal.class) {
                                BigDecimal value = rs.getBigDecimal(columnName);
                                if (value != null)
                                    f.set(pojo, value);
                            } else if (type == Float.class) {
                                Float value = rs.getFloat(columnName);
                                if (value != null)
                                    f.set(pojo, value);
                            } else if (type == java.sql.Array.class) {
                                java.sql.Array value = rs.getArray(columnName);
                                if (value != null)
                                    f.set(pojo, value);
                            } else if (type == Blob.class) {
                                Blob value = rs.getBlob(columnName);
                                if (value != null)
                                    f.set(pojo, value);
                            } else if (type == Clob.class) {
                                Clob value = rs.getClob(columnName);
                                if (value != null)
                                    f.set(pojo, value);
                            }
                        } else {
                            LOG.debug("Empty value has been declared for the Column Name of field:" + f.getName()
                                    + "- class:" + pojoCls);
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Can't assign value to field:" + f.getName() + ", of class:" + pojoCls, e);
                }
            }
            return null;
        };
    }
}
