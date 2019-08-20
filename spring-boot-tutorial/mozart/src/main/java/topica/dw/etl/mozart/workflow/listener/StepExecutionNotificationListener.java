package topica.dw.etl.mozart.workflow.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Date;

public class StepExecutionNotificationListener extends StepExecutionListenerSupport {
    private static final Logger logger = LoggerFactory.getLogger(StepExecutionNotificationListener.class);
    private String tableName;
    private DataSource ds;

    public StepExecutionNotificationListener(String tableName, DataSource ds) {
        this.tableName = tableName;
        this.ds = ds;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        int countItemRead = stepExecution.getReadCount();
        int countItemWrite = stepExecution.getWriteCount();
        Date timeStart = stepExecution.getStartTime();
        Date timeFinish = stepExecution.getEndTime();
        logger.info("After step ... of table:" + tableName + "\n read record: " + countItemRead + "\n write record: "
                + countItemWrite + "\n timeStart: " + timeStart + "\n timeFinish: " + timeFinish);
        try {
            Connection con = ds.getConnection();
            if (con != null) {
                String sqlInsert = "insert into log_run_job(table_name,item_read,item_write,created_at,updated_at)"
                        + "values(?,?,?,?,?)";
                PreparedStatement prepariedStmt = con.prepareStatement(sqlInsert);
                prepariedStmt.setString(1, tableName);
                prepariedStmt.setInt(2, countItemRead);
                prepariedStmt.setInt(3, countItemWrite);
                prepariedStmt.setString(4, (new Date()).toString());
                prepariedStmt.setString(5, (new Date()).toString());
                prepariedStmt.execute();
            }
            con.close();
        } catch (Exception e) {
            logger.info("error insert log..." + e);
        }
        return super.afterStep(stepExecution);
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before step ... of table:" + tableName);
        super.beforeStep(stepExecution);
    }
}
