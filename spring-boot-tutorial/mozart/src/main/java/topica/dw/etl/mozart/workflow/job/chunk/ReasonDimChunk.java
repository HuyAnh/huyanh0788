package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.domain.ReasonDim;

@Component("reason_dim_chunk")
public class ReasonDimChunk extends SqlGeneratorChunkStepBuilder<ReasonDim, ReasonDim> {
    public ReasonDimChunk() {
        super(ReasonDim.class, ReasonDim.class, "reason_dim_chunk");
    }
}