package topica.dw.etl.mozart.workflow.job.tasklet;

import org.springframework.stereotype.Component;

@Component("no_course_minerva_not_c3_tele_sale_detail_tasklet")
public class NoCourseMinervaNotC3TeleSaleDetailTasklet extends BaseSaleDetailTasklet{
    private static final String INSERT_SQL_FILE = "/etl/sql/insert_batched_no_course_minerva_not_c3_tele_sale_details.sql";
    private static final String DELETE_SQL_FILE = "/etl/sql/delete_batched_no_course_minerva_not_c3_tele_sale_details.sql";
    private static final String SOURCE = "MINERVA - NOT C3 TELE";
    private static final String TYPE = "NO COURSE";

    public NoCourseMinervaNotC3TeleSaleDetailTasklet() {
        super(INSERT_SQL_FILE, DELETE_SQL_FILE, SOURCE, TYPE);
    }
}
