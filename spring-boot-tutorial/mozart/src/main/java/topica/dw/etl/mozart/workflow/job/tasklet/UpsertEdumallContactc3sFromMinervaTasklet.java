package topica.dw.etl.mozart.workflow.job.tasklet;

import org.springframework.stereotype.Component;

@Component("upsert_edumall_contactc3s_from_minerva_tasklet")
public class UpsertEdumallContactc3sFromMinervaTasklet extends BaseInsertOrUpsertTasklet{
    private static final String INSERT_SQL_FILE = "/etl/sql/upsert_edumall_contactc3s_from_minerva.sql";
    private static final String SOURCE = "EDUMALL_CONTACTC3S_MINERVA";

    public UpsertEdumallContactc3sFromMinervaTasklet() {
        super(INSERT_SQL_FILE, SOURCE, UpsertEdumallContactc3sFromMinervaTasklet.class);
    }
}