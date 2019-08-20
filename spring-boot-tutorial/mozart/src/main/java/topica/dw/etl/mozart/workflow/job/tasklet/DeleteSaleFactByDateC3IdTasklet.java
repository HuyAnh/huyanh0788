package topica.dw.etl.mozart.workflow.job.tasklet;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.common.IOCommon;
import topica.dw.etl.mozart.workflow.common.ulti.Helper;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

@Component("delete_sale_fact_by_date_c3_id_tasklet")
public class DeleteSaleFactByDateC3IdTasklet implements Tasklet {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteSaleFactByDateC3IdTasklet.class);
    private static final String SQL_DATE_C3_SALE_DETAIL_TASKLET = "/etl/sql/list_date_c3_id_from_sale_details_changed.sql";
    private static final String SQL_DELETE_SALE_FACT = "/etl/sql/delete_sale_fact_by_date_c3_id.sql";
    private static final String DATE_C3_ID = "date_c3_id";

    private final DataSource kuduDs;

    private final DataSource warehouseDS;

    @Autowired
    public DeleteSaleFactByDateC3IdTasklet(@Qualifier("stagingDs") DataSource kuduDs, @Qualifier("warehouseDS") DataSource warehouseDS) {
        this.kuduDs = kuduDs;
        this.warehouseDS = warehouseDS;
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        try (Connection connKudu = kuduDs.getConnection();
             Connection connWareHouse = warehouseDS.getConnection();
             Statement statementKudu = connKudu.createStatement();
             Statement statementWareHouse = connWareHouse.createStatement()) {

            String sqlDateC3SaleDetail = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(SQL_DATE_C3_SALE_DETAIL_TASKLET));
            String sqlDeleteSaleFact = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(SQL_DELETE_SALE_FACT));

            String dateC3Ids = Helper.processListToString(Helper.buildReadSql(statementKudu, sqlDateC3SaleDetail, DATE_C3_ID));
            sqlDeleteSaleFact = StringUtils.replaceAll(sqlDeleteSaleFact,"\\?1", dateC3Ids);

            LOG.info("Sql delete sale fact: {} ", sqlDeleteSaleFact);
            int rowSaleFactDeleted = statementWareHouse.executeUpdate(sqlDeleteSaleFact);
            LOG.info("Số bản ghi sale fact đã xóa: {}", rowSaleFactDeleted);

        } catch (Exception ex) {
            LOG.error("Date C3 Sale Detail Tasklet {0}:", ex);
            throw ex;
        }
        return RepeatStatus.FINISHED;
    }


}