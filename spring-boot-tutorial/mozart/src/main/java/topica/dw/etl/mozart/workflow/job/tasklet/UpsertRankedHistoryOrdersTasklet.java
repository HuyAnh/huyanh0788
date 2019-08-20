package topica.dw.etl.mozart.workflow.job.tasklet;

import org.springframework.stereotype.Component;

@Component("upsert_ranked_history_orders_tasklet")
public class UpsertRankedHistoryOrdersTasklet extends BaseInsertOrUpsertTasklet {

    private static final String INSERT_SQL_FILE = "/etl/sql/upsert_ranked_history_orders.sql";
    private static final String SOURCE = "RANKED_HISTORY_ORDERS";

    public UpsertRankedHistoryOrdersTasklet() {
        super(INSERT_SQL_FILE, SOURCE, UpsertRankedHistoryOrdersTasklet.class);
    }
}