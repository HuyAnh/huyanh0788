package topica.dw.etl.mozart.workflow.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation that can be used to define the Data Warehouse table name
 * to generate SQL query dump data into Data Warehouse on runtime
 * (depending on its signature),
 * <p>
 * Must be declared value when used this
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface DWTable {
    String value();
}
