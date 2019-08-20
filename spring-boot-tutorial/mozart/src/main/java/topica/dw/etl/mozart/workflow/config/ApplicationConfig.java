package topica.dw.etl.mozart.workflow.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import topica.dw.etl.mozart.workflow.reader.JsonStringRowMapper;

@Configuration
@Import({SpringBatchConfig.class})
@ComponentScan({"topica.dw.etl.mozart.workflow.config", "topica.dw.etl.mozart.workflow.service",
        "topica.dw.etl.mozart.workflow.event", "topica.dw.etl.mozart.workflow.executor",
        "topica.dw.etl.mozart.workflow.job"})
public class ApplicationConfig {
    @Bean(name = "app_executor")
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor("task_executor");
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, true);
        return mapper;
    }

    @Bean
    public JsonStringRowMapper jsonRowMapper(ObjectMapper objMapper) {
        return new JsonStringRowMapper(objMapper);
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertyConfigInDev() {
        return new PropertySourcesPlaceholderConfigurer();
    }
}
