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
import java.util.ArrayList;
import java.util.List;

@Component("delete_marketing_fact_by_date_c3_id_tasklet")
public class DeleteMarketingFactByDateC3IdTasklet implements Tasklet {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteMarketingFactByDateC3IdTasklet.class);
    private static final String SQL_DATE_C3_ID_FROM_SALE_DETAILS = "/etl/sql/list_date_c3_id_from_sale_details_changed.sql";
    private static final String SQL_DATE_C3_ID_FROM_NAMI_AD_BYDATES = "/etl/sql/list_date_c3_id_from_nami_ad_bydates_changed.sql";
    private static final String SQL_DELETE_MARKETING_FACT = "/etl/sql/delete_marketing_fact_by_date_c3_id.sql";
    private static final String DATE_C3_ID = "date_c3_id";

    private final DataSource kuduDs;

    private final DataSource warehouseDS;

    @Autowired
    public DeleteMarketingFactByDateC3IdTasklet(@Qualifier("stagingDs") DataSource kuduDs, @Qualifier("warehouseDS") DataSource warehouseDS) {
        this.kuduDs = kuduDs;
        this.warehouseDS = warehouseDS;
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        try (Connection connKudu = kuduDs.getConnection();
             Connection connWareHouse = warehouseDS.getConnection();
             Statement statementKudu = connKudu.createStatement();
             Statement statementWareHouse = connWareHouse.createStatement()) {

            String sqlDateC3IdSaleDetails = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(SQL_DATE_C3_ID_FROM_SALE_DETAILS));
            String sqlDateC3IdNamiAdBydates = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(SQL_DATE_C3_ID_FROM_NAMI_AD_BYDATES));
            String sqlDeleteMarketingFact = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(SQL_DELETE_MARKETING_FACT));

            List<String> listDateC3s = new ArrayList<>();

            listDateC3s.addAll(Helper.buildReadSql(statementKudu, sqlDateC3IdSaleDetails, DATE_C3_ID));
            listDateC3s.addAll(Helper.buildReadSql(statementKudu, sqlDateC3IdNamiAdBydates, DATE_C3_ID));

            String dateC3Ids = Helper.processListToString(listDateC3s);
            sqlDeleteMarketingFact = StringUtils.replaceAll(sqlDeleteMarketingFact,"\\?1", dateC3Ids);

            LOG.info("Sql delete marketing fact: {}", sqlDeleteMarketingFact);
            int rowMarketingFactDeleted = statementWareHouse.executeUpdate(sqlDeleteMarketingFact);
            LOG.info("Số bản ghi marketing fact đã xóa: {}", rowMarketingFactDeleted);

        } catch (Exception ex) {
            LOG.error("Delete Date C3 Marketing Fact Tasklet {0}:", ex);
            throw ex;
        }
        return RepeatStatus.FINISHED;
    }

}
