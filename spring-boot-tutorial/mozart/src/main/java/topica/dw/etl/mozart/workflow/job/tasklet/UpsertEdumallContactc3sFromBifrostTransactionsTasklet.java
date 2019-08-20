package topica.dw.etl.mozart.workflow.job.tasklet;

import org.springframework.stereotype.Component;

@Component("upsert_edumall_contactc3s_from_bifrost_transactions_tasklet")
public class UpsertEdumallContactc3sFromBifrostTransactionsTasklet extends BaseInsertOrUpsertTasklet {
    private static final String INSERT_SQL_FILE = "/etl/sql/upsert_edumall_contactc3s_from_bifrost_transactions.sql";
    private static final String SOURCE = "EDUMALL_CONTACTC3S_BIFOST";

    public UpsertEdumallContactc3sFromBifrostTransactionsTasklet() {
        super(INSERT_SQL_FILE, SOURCE, UpsertEdumallContactc3sFromBifrostTransactionsTasklet.class);
    }
}