package topica.dw.etl.mozart.workflow.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import topica.dw.etl.mozart.workflow.writer.DwWriterProviderFactory;

import javax.sql.DataSource;

@Configuration
@PropertySource({"classpath:application.properties"})
public class EdumallDwDatasourceConfig {

    @Value("${edumall.dw.jdbc.driverclass}")
    private String dataWareHouseDriverClass;

    @Value("${edumall.dw.jdbc.url}")
    private String dataWareHouseUrl;

    @Value("${edumall.dw.jdbc.username}")
    private String userName;

    @Value("${edumall.dw.jdbc.password}")
    private String passWord;

    @Value("${edumall.dw.provider.name}")
    private String writerProviderName;

    @Bean(name = "warehouseDS")
    public DataSource dataSource() throws Exception {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(dataWareHouseDriverClass);
        dataSource.setUrl(dataWareHouseUrl);
        dataSource.setUsername(userName);
        dataSource.setPassword(passWord);
        return dataSource;
    }

    @Bean(name = "dwTargetProvider")
    public DwWriterProviderFactory.WriterProvider dwTarget() throws Exception {
        DwWriterProviderFactory.WriterProvider providerAlias = DwWriterProviderFactory.WriterProvider.findByName(writerProviderName);
        return providerAlias;
    }
}
