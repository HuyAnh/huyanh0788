package topica.dw.etl.mozart.workflow.service;

import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import topica.dw.etl.mozart.workflow.common.exception.UnsupportedBuildJobException;
import topica.dw.etl.mozart.workflow.job.EtlJobBuilder;

import java.util.List;

@Service
public class CommonEtlJobBuilderService {

    private static MapJobBuilder mapBuilder;

    @Autowired
    public void setMapBuilder(MapJobBuilder mapBuilder) {
        CommonEtlJobBuilderService.mapBuilder = mapBuilder;
    }

    public static Job lookupJob(String jobName) throws UnsupportedBuildJobException {
        return mapBuilder.findJobByName(jobName);
    }

    @Component
    private static class MapJobBuilder {

        @Autowired
        private List<EtlJobBuilder> listJobBuilder;

        /**
         * Find an existing job
         *
         * @param jobName Name of the executing job in the batch.
         * @return An object representing the etl job.
         */
        private EtlJobBuilder getEtlJobBuilder(String jobName) throws UnsupportedBuildJobException {
            if (StringUtils.isBlank(jobName))
                throw new UnsupportedBuildJobException("Cannot find the job because Job name is empty");
            return listJobBuilder.stream().filter(e -> jobName.equalsIgnoreCase(e.getJobName())).findAny().map(x -> x)
                    .orElseThrow(() -> new UnsupportedBuildJobException("Cannot find the job: " + jobName));
        }

        /**
         * Find ETL job builder and after that call method to setup job
         *
         * @param jobName Job name provided by azkaban. It is also azkaban's step
         *                name.
         * @return Job
         * @throws UnsupportedBuildJobException Any issue in initializing the etl job.
         */
        private Job findJobByName(String jobName) throws UnsupportedBuildJobException {
            EtlJobBuilder builder = getEtlJobBuilder(jobName);
            try {
                return builder.buildEtlJob();
            } catch (Exception e) {
                throw new UnsupportedBuildJobException("Have a problem when initiate job", e);
            }

        }

    }
}
