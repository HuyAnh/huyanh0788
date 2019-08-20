package topica.dw.etl.mozart.workflow.job.tasklet;

import org.springframework.stereotype.Component;

@Component("minerva_c3_tele_sale_detail_tasklet")
public class MinervaC3TeleSaleDetailTasklet extends BaseSaleDetailTasklet {

    private static final String INSERT_SQL_FILE = "/etl/sql/insert_batched_minerva_c3_tele_sale_details.sql";
    private static final String DELETE_SQL_FILE = "/etl/sql/delete_batched_minerva_c3_tele_sale_details.sql";
    private static final String SOURCE = "MINERVA - C3 TELE";
    private static final String TYPE = "COURSE";

    public MinervaC3TeleSaleDetailTasklet() {
        super(INSERT_SQL_FILE, DELETE_SQL_FILE, SOURCE, TYPE);
    }

}
