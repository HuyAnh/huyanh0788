package topica.dw.etl.mozart.workflow.job.tasklet;

import org.springframework.stereotype.Component;

@Component("upsert_edumall_contactc3s_from_nami_tasklet")
public class UpsertEdumallContactc3sFromNamiTasklet extends BaseInsertOrUpsertTasklet{
    private static final String INSERT_SQL_FILE = "/etl/sql/upsert_edumall_contactc3s_from_nami.sql";
    private static final String SOURCE = "EDUMALL_CONTACTC3S_NAMI";

    public UpsertEdumallContactc3sFromNamiTasklet() {
        super(INSERT_SQL_FILE, SOURCE, UpsertEdumallContactc3sFromNamiTasklet.class);
    }
}
