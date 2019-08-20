package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.domain.AreaDim;

@Component("area_dim_chunk")
public class AreaDimChunk extends SqlGeneratorChunkStepBuilder<AreaDim, AreaDim> {
    public AreaDimChunk() {
        super(AreaDim.class, AreaDim.class, "area_dim_chunk");
    }
}
