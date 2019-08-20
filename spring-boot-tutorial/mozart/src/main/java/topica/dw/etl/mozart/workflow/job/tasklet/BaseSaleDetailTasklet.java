package topica.dw.etl.mozart.workflow.job.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import topica.dw.etl.mozart.workflow.common.IOCommon;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BaseSaleDetailTasklet implements Tasklet {

    private static final Logger LOG = LoggerFactory.getLogger(BaseSaleDetailTasklet.class);

    private String insertSqlFile;
    private String deleteSqlFile;
    private String source;
    private String type;

    protected BaseSaleDetailTasklet(String insertSqlFile, String deleteSqlFile, String source, String type) {
        this.insertSqlFile = insertSqlFile;
        this.deleteSqlFile = deleteSqlFile;
        this.source = source;
        this.type = type;
    }

    @Autowired
    @Qualifier("stagingDs")
    DataSource staging;

    private int deleteSaleDetails(Connection connection) throws SQLException, IOException {
        final String QUERY = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(deleteSqlFile));

        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY)) {
            return preparedStatement.executeUpdate();
        }
    }

    private int insertSaleDetails(Connection connection) throws SQLException, IOException {
        final String QUERY = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(insertSqlFile));

        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY)) {
            return preparedStatement.executeUpdate();
        }
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        try (Connection connection = staging.getConnection()) {
            /** Needs batch info. */
            LOG.info("Source: {}, type: {} - Started replacing sale details.", source, type);
            deleteSaleDetails(connection);
            LOG.info("Source: {}, type: {} - Deleted records from sale_details successfully.", source, type);
            insertSaleDetails(connection);
            LOG.info("Source: {}, type: {} - Inserted records into sale_details successfully.", source, type);
            LOG.info("Source: {}, type: {} - Finished replacing sale details.", source, type);
        }

        return RepeatStatus.FINISHED;
    }
}
