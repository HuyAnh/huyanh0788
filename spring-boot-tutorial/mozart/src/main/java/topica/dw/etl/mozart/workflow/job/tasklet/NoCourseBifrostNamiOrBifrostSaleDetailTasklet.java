package topica.dw.etl.mozart.workflow.job.tasklet;

import org.springframework.stereotype.Component;

@Component("no_course_bifrost_nami_or_bifrost_sale_detail_tasklet")
public class NoCourseBifrostNamiOrBifrostSaleDetailTasklet extends BaseSaleDetailTasklet {

    private static final String INSERT_SQL_FILE = "/etl/sql/insert_batched_no_course_bifrost_nami_or_bifrost_sale_details.sql";
    private static final String DELETE_SQL_FILE = "/etl/sql/delete_batched_no_course_bifrost_nami_or_bifrost_sale_details.sql";
    private static final String SOURCE = "BIFROST_NAMI / BIFROST";
    private static final String TYPE = "NO COURSE";

    public NoCourseBifrostNamiOrBifrostSaleDetailTasklet() {
        super(INSERT_SQL_FILE, DELETE_SQL_FILE, SOURCE, TYPE);
    }

}
