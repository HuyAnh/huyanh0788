package topica.dw.etl.mozart.workflow.common;

import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class ReflectionCommon {

    public static List<Field> getAllDeclaredFieldsOfAnnotation(final Class<?> cls,
                                                               final Class<? extends Annotation> annotationCls) {
        List<Field> results = new ArrayList<>();
        results.addAll(FieldUtils.getFieldsListWithAnnotation(cls, annotationCls));
        return results;
    }


}
