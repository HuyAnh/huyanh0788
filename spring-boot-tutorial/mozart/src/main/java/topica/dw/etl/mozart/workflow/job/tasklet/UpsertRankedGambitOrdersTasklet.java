package topica.dw.etl.mozart.workflow.job.tasklet;

import org.springframework.stereotype.Component;

@Component("upsert_ranked_gambit_orders_tasklet")
public class UpsertRankedGambitOrdersTasklet extends BaseInsertOrUpsertTasklet {
    private static final String INSERT_SQL_FILE = "/etl/sql/upsert_ranked_gambit_orders.sql";
    private static final String SOURCE = "RANKED_GAMBIT_ORDERS";

    public UpsertRankedGambitOrdersTasklet() {
        super(INSERT_SQL_FILE, SOURCE, UpsertRankedGambitOrdersTasklet.class);
    }
}
