package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.domain.DepartmentDim;


@Component("department_dim_chunk")
public class DepartmentDimChunk extends SqlGeneratorChunkStepBuilder<DepartmentDim, DepartmentDim> {
    public DepartmentDimChunk() {
        super(DepartmentDim.class, DepartmentDim.class, "department_dim_chunk");
    }
}
