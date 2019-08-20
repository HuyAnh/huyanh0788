package topica.dw.etl.mozart.workflow.job.tasklet;

import org.springframework.stereotype.Component;

@Component("upsert_edumall_contactc3s_from_eros_tasklet")
public class UpsertEdumallContactc3sFromErosTasklet extends BaseInsertOrUpsertTasklet{
    private static final String INSERT_SQL_FILE = "/etl/sql/upsert_edumall_contactc3s_from_eros.sql";
    private static final String SOURCE = "EDUMALL_CONTACTC3S_EROS";

    public UpsertEdumallContactc3sFromErosTasklet() {
        super(INSERT_SQL_FILE, SOURCE, UpsertEdumallContactc3sFromErosTasklet.class);
    }
}
