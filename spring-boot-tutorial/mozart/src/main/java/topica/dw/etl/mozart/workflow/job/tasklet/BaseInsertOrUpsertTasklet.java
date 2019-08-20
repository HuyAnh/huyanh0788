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

public class BaseInsertOrUpsertTasklet implements Tasklet {

    private String insertSqlFile;

    private String source;

    private Class logClass;

    public BaseInsertOrUpsertTasklet(String insertSqlFile, String source, Class logClass) {
        this.insertSqlFile = insertSqlFile;
        this.source = source;
        this.logClass = logClass;
    }

    @Autowired
    @Qualifier("stagingDs")
    DataSource staging;

    private int insertView(Connection connection) throws SQLException, IOException {
        final String QUERY = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(insertSqlFile));

        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY)) {
            return preparedStatement.executeUpdate();
        }
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        try (Connection connection = staging.getConnection()) {
            /** Needs batch info. */
            final Logger log = LoggerFactory.getLogger(logClass);
            log.info("Source: {} - Start insert records", source);
            insertView(connection);
            log.info("Source: {} - Finished insert records", source);
        }

        return RepeatStatus.FINISHED;
    }
}
