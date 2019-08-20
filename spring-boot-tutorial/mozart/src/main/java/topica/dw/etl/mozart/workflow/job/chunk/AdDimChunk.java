package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.domain.AdDim;

@Component("ad_dim_chunk")
public class AdDimChunk extends SqlGeneratorChunkStepBuilder<AdDim, AdDim> {
    public AdDimChunk() {
        super(AdDim.class, AdDim.class, "ad_dim_chunk");
    }
}
