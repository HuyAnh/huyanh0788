package topica.dw.etl.mozart.workflow.job.tasklet;

import org.springframework.stereotype.Component;

@Component("insert_log_mozart_transformation_tasklet")
public class InsertLogMozartTransformationTasklet extends BaseInsertOrUpsertTasklet {
    private static final String INSERT_SQL_FILE = "/etl/sql/insert_log_mozart_transformation_tasklet.sql";
    private static final String SOURCE = "LOG_MOZART_TRANSFORMATION";

    public InsertLogMozartTransformationTasklet() {
        super(INSERT_SQL_FILE, SOURCE, InsertLogMozartTransformationTasklet.class);
    }
}
