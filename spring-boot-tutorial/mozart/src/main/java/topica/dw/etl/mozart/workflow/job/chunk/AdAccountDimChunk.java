package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.domain.AdAccountDim;

@Component("ad_account_dim_chunk")
public class AdAccountDimChunk extends SqlGeneratorChunkStepBuilder<AdAccountDim, AdAccountDim> {

    public AdAccountDimChunk() {
        super(AdAccountDim.class, AdAccountDim.class, "ad_account_dim_chunk");
    }
}
