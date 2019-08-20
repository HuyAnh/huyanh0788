package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.domain.CourseDim;

@Component("course_dim_chunk")
public class CourseDimChunk extends SqlGeneratorChunkStepBuilder<CourseDim, CourseDim> {
    public CourseDimChunk() {
        super(CourseDim.class, CourseDim.class, "course_dim_chunk");
    }
}
