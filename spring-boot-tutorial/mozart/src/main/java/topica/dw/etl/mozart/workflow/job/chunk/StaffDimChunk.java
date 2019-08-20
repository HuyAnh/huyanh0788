package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.domain.StaffDim;

@Component("staff_dim_chunk")
public class StaffDimChunk extends SqlGeneratorChunkStepBuilder<StaffDim, StaffDim> {
    public StaffDimChunk() {
        super(StaffDim.class, StaffDim.class, "staff_dim_chunk");
    }
}
