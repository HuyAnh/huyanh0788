package topica.dw.etl.mozart.workflow.job.tasklet;

import org.springframework.stereotype.Component;

@Component("upsert_ranked_bifrost_transactions_tasklet")
public class UpsertRankedBifrostTransactionsTasklet extends BaseInsertOrUpsertTasklet {
    private static final String INSERT_SQL_FILE = "/etl/sql/upsert_ranked_bifrost_transactions.sql";
    private static final String SOURCE = "RANKED_BIFROST_TRANSACTIONS";

    public UpsertRankedBifrostTransactionsTasklet() {
        super(INSERT_SQL_FILE, SOURCE, UpsertRankedBifrostTransactionsTasklet.class);
    }
}
