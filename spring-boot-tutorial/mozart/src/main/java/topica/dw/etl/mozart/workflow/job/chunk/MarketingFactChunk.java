package topica.dw.etl.mozart.workflow.job.chunk;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.common.IOCommon;
import topica.dw.etl.mozart.workflow.common.ulti.Helper;
import topica.dw.etl.mozart.workflow.domain.MarketingFact;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

@Component("marketing_fact_chunk")
public class MarketingFactChunk extends SqlGeneratorChunkStepBuilder<MarketingFact, MarketingFact> {
    private static final Logger LOG = LoggerFactory.getLogger(SaleFactChunk.class);
    private static final String SQL_LIST_DATE_C3_ID_FROM_SALE_DETAILS_CHANGED = "/etl/sql/list_date_c3_id_from_sale_details_changed.sql";
    private static final String SQL_LIST_DATE_C3_ID_FROM_NAMI_AD_BYDATES_CHANGED = "/etl/sql/list_date_c3_id_from_sale_details_changed.sql";
    private static final String SQL_MARKETING_FACT_CHUNK = "/etl/sql/marketing_fact_chunk.sql";
    private static final String DATE_C3_ID = "date_c3_id";

    @Autowired
    @Qualifier("stagingDs")
    private DataSource stagingDs;

    public MarketingFactChunk() {
        super(MarketingFact.class, MarketingFact.class, "marketing_fact_chunk");
    }

    @Override
    protected String buildReadSql() throws Exception {
        try (Connection connKudu = stagingDs.getConnection();
             Statement statementKudu = connKudu.createStatement()) {

            String sqlListDateC3IdFromSaleDetailsChanged = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(SQL_LIST_DATE_C3_ID_FROM_SALE_DETAILS_CHANGED));
            String sqlListDateC3IdFromNamiAdByDatesChanged = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(SQL_LIST_DATE_C3_ID_FROM_NAMI_AD_BYDATES_CHANGED));
            String sqlFact = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(SQL_MARKETING_FACT_CHUNK));

            sqlFact = Helper.buildSqlFact(statementKudu, sqlFact, sqlListDateC3IdFromSaleDetailsChanged, DATE_C3_ID, "\\?1");
            sqlFact = Helper.buildSqlFact(statementKudu, sqlFact, sqlListDateC3IdFromNamiAdByDatesChanged, DATE_C3_ID, "\\?2");

            LOG.info("sql update sale fact: {}", sqlFact);
            return sqlFact;

        } catch (Exception e) {
            LOG.error("marketing fact chunk exception {}", e.getMessage());
            throw e;
        }
    }

}
