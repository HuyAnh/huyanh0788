package topica.dw.etl.mozart.workflow.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation that can be used to map The column name of Data Source
 * Table Table with field of the java domain object (depending on its
 * signature),
 * <p>
 * Must be declared name when used this
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface DWTableColumn {
    /**
     * Value of column name
     *
     * @return
     */
    String value();

    /**
     * The signal to indicate the value will not be assigned from external
     * resource
     *
     * @return
     */
    boolean ignoreAssign() default false;
}
