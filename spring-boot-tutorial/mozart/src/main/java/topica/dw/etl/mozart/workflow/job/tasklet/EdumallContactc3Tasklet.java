package topica.dw.etl.mozart.workflow.job.tasklet;

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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;

@Component("edumall_contactc3_tasklet")
public class EdumallContactc3Tasklet implements Tasklet {

    private static final Logger LOG = LoggerFactory.getLogger(StartTimeTasklet.class);
    private static final String SQL_FILE_MINERVA_CONTACTC3 = "/etl/sql/upsert_edumall_contactc3s_from_minerva.sql";
    private static final String SQL_FILE_NAMI_CONTACTC3 = "/etl/sql/upsert_edumall_contactc3s_from_nami.sql";
    private static final String SQL_FILE_BIFROST_TRANSACTION = "/etl/sql/upsert_edumall_contactc3s_from_bifrost_transactions.sql";
    private static final String SQL_FILE_EROS = "/etl/sql/upsert_edumall_contactc3s_from_eros.sql";

    @Autowired
    @Qualifier("stagingDs")
    private DataSource stagingDs;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        LOG.info("Upsert edumall_contactc3s");
        Connection con = null;
        PreparedStatement psmtMinerva = null;
        PreparedStatement psmtNami = null;
        PreparedStatement psmtBifrost = null;
        PreparedStatement psmtEros = null;
        try {

            con = stagingDs.getConnection();

            String sqlMinervaContactc3 = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(SQL_FILE_MINERVA_CONTACTC3));
            psmtMinerva = con.prepareStatement(sqlMinervaContactc3);

            String sqlNamiContactc3 = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(SQL_FILE_NAMI_CONTACTC3));
            psmtNami = con.prepareStatement(sqlNamiContactc3);

            String sqlBifrostTransaction = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(SQL_FILE_BIFROST_TRANSACTION));
            psmtBifrost = con.prepareStatement(sqlBifrostTransaction);

            String sqlEros = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(SQL_FILE_EROS));
            psmtEros = con.prepareStatement(sqlEros);

            boolean minerva = psmtMinerva.execute();
            boolean nami = psmtNami.execute();
            boolean bifrost = psmtBifrost.execute();
            boolean eros = psmtEros.execute();

            LOG.info("upsert minerva is: {}", minerva);
            LOG.info("upsert nami is: {}", nami);
            LOG.info("upsert bifrost is: {}", bifrost);
            LOG.info("upsert eros is: {}", eros);

        } catch (Exception e) {
            LOG.error("Edumall contactc3 exception");
            LOG.error(e.getMessage());
            throw e;
        } finally {
            psmtMinerva.close();
            psmtNami.close();
            psmtBifrost.close();
            psmtEros.close();
            con.close();
        }
        return RepeatStatus.FINISHED;
    }
}
