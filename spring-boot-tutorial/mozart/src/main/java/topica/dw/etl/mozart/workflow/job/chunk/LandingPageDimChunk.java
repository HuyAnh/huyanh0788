package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.domain.LandingPageDim;

@Component("landing_page_dim_chunk")
public class LandingPageDimChunk extends SqlGeneratorChunkStepBuilder<LandingPageDim, LandingPageDim> {
    public LandingPageDimChunk() {
        super(LandingPageDim.class, LandingPageDim.class, "landing_page_dim_chunk");
    }
}
