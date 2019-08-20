package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.domain.PartnerDim;

@Component("partner_dim_chunk")
public class PartnerDimChunk extends SqlGeneratorChunkStepBuilder<PartnerDim, PartnerDim> {
    public PartnerDimChunk() {
        super(PartnerDim.class, PartnerDim.class, "partner_dim_chunk");
    }
}
