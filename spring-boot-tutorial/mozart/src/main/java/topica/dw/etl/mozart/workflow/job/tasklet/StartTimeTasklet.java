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
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

@Component("start_time_tasklet")
public class StartTimeTasklet implements Tasklet {

    private static final Logger LOG = LoggerFactory.getLogger(StartTimeTasklet.class);
    private static final String SQL_FILE_MOZART_TRANSFORMATION_UPDATE = "/etl/sql/mozart_transformation_update.sql";
    private static final String SQL_FILE_MOZART_TRANSFORMATION_SELECT = "/etl/sql/mozart_transformation_select.sql";

    @Autowired
    @Qualifier("stagingDs")
    private DataSource stagingDs;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        LOG.info("Update start time ... ... ...");
        Connection con = null;
        PreparedStatement psmt = null;
        ResultSet resultSet = null;
        try {
            String sqlUpdateStartTime = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(SQL_FILE_MOZART_TRANSFORMATION_UPDATE));
            String sqlGetInfoEndTime = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(SQL_FILE_MOZART_TRANSFORMATION_SELECT));

            con = stagingDs.getConnection();

            psmt = con.prepareStatement(sqlGetInfoEndTime);
            psmt.setString(1, "end_time");

            resultSet = psmt.executeQuery();
            Timestamp endTime = null;
            while (resultSet.next()) {
                endTime = resultSet.getTimestamp("info");
            }

            if (endTime != null) {
                LOG.info(endTime.toString());
            } else {
                LOG.error("End time is null");
            }

            Timestamp now = new Timestamp(Instant.now(Clock.systemUTC()).toEpochMilli());
            psmt = con.prepareStatement(sqlUpdateStartTime);

            psmt.setTimestamp(1, endTime);
            psmt.setString(2, "start_time_tasklet");
            psmt.setTimestamp(3, now);
            psmt.setString(4, "start_time");

            int rs = psmt.executeUpdate();

            LOG.info("Updated: " + rs);

        } catch (Exception e) {
            LOG.info(e.getMessage());
        } finally {
            resultSet.close();
            psmt.close();
            con.close();
        }
        return RepeatStatus.FINISHED;
    }
}
