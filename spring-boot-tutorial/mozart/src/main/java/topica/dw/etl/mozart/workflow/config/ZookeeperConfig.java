package topica.dw.etl.mozart.workflow.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.task.TaskExecutor;
import topica.dw.etl.mozart.workflow.common.zookeeper.AlohomoraTasksControl;
import topica.dw.etl.mozart.workflow.common.zookeeper.ZooMaster;
import topica.dw.etl.mozart.workflow.common.zookeeper.ZooMetricsDataProvider;
import topica.dw.etl.mozart.workflow.common.zookeeper.ZooWorker;

import java.net.InetAddress;
import java.util.Random;

@Configuration
public class ZookeeperConfig {

    @Value("${zookeeper.url}")
    private String zookeeperUrl;

    @Value("${zookeeper.client.timeout}")
    private Integer clientTimeout;

    @Value("${etl.task.num.executor}")
    private Integer numOfTaskExecutor;

    @Bean(name = "zoo_master")
    @DependsOn({"zoo_metrics_provider", "zoo_worker"})
    public ZooMaster zooMaster(@Qualifier("server_id") String serverId) {
        return new ZooMaster(zookeeperUrl, clientTimeout, serverId);
    }

    @Bean(name = "zoo_worker")
    @DependsOn({"alohomora_etl_task_control"})
    public ZooWorker zooTask(@Qualifier("app_executor") TaskExecutor taskExecutor,
                             @Qualifier("server_id") String serverId) throws Exception {
        return new ZooWorker(zookeeperUrl, clientTimeout, taskExecutor, serverId);
    }

    @Bean("zoo_metrics_provider")
    public ZooMetricsDataProvider dataProvider(@Qualifier("server_id") String serverId) throws Exception {
        return new ZooMetricsDataProvider(zookeeperUrl, clientTimeout, serverId);
    }

    @Bean("alohomora_etl_task_control")
    public AlohomoraTasksControl etlTaskControl(@Qualifier("server_id") String serverId) {
        return new AlohomoraTasksControl(serverId, numOfTaskExecutor);
    }

    @Bean(name = "server_id")
    public String generateServerId() {
        InetAddress ip;
        try {
            ip = InetAddress.getLocalHost();
            return ip.getHostName();
        } catch (Exception e) {
            // By default: generate random Integer and assign as server id
            return Integer.toHexString((new Random()).nextInt());
        }
    }
}
