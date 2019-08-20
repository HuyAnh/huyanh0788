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
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

@Component("end_time_tasklet")
public class EndTimeTasklet implements Tasklet {
    private static final Logger LOG = LoggerFactory.getLogger(StartTimeTasklet.class);
    private static final String SQL_FILE = "/etl/sql/mozart_transformation_update.sql";

    @Autowired
    @Qualifier("stagingDs")
    private DataSource stagingDs;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        LOG.info("Update end time ... ... ...");
        try {
            String sql = IOCommon.readSqlFileInputStream(this.getClass().getResourceAsStream(SQL_FILE));
            Timestamp now = new Timestamp(Instant.now(Clock.systemUTC()).toEpochMilli());
            Connection con = stagingDs.getConnection();
            PreparedStatement psmt = con.prepareStatement(sql);
            psmt.setTimestamp(1, now);
            psmt.setString(2, "end_time_tasklet");
            psmt.setTimestamp(3, now);
            psmt.setString(4, "end_time");
            int rs = psmt.executeUpdate();
            LOG.info("Updated: {}" + rs);
            psmt.close();
            con.close();
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }
        return RepeatStatus.FINISHED;
    }
}
