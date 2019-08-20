package topica.dw.etl.mozart.workflow.config;

import com.cloudera.impala.jdbc4.DataSource;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class ImpalaDataSourceConfig {
    private final Logger log = Logger.getLogger(ImpalaDataSourceConfig.class);

    @Value("${kudu.impala.driverclass}")
    private String impalaDriverClass;

    @Value("${kudu.impala.url}")
    private String impalaUrl;

    @Bean(name = "stagingDs")
    public javax.sql.DataSource dataSource() {
        try {
            Class.forName(impalaDriverClass);
            DataSource ds = new DataSource();
            ds.setURL(impalaUrl);
            return ds;
        } catch (Exception e) {
            log.error(e);
            return null;
        }
    }

    @Bean(name = "impalaJdbcTemplate")
    public JdbcTemplate impalaJdbcTemplate(@Qualifier("stagingDs") DataSource impalaDs) {
        return new JdbcTemplate(impalaDs);
    }
}
