package topica.dw.etl.mozart.workflow.job.tasklet;

import org.springframework.stereotype.Component;

@Component("bifrost_nami_or_bifrost_sale_detail_tasklet")
public class BifrostNamiOrBifrostSaleDetailTasklet extends BaseSaleDetailTasklet {

    private static final String INSERT_SQL_FILE = "/etl/sql/insert_batched_bifrost_nami_or_bifrost_sale_details.sql";
    private static final String DELETE_SQL_FILE = "/etl/sql/delete_batched_bifrost_nami_or_bifrost_sale_details.sql";
    private static final String SOURCE = "BIFROST_NAMI / BIFROST";
    private static final String TYPE = "COURSE";

    public BifrostNamiOrBifrostSaleDetailTasklet() {
        super(INSERT_SQL_FILE, DELETE_SQL_FILE, SOURCE, TYPE);
    }
}
