package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.domain.CampaignDim;

@Component("campaign_dim_chunk")
public class CampaignDimChunk extends SqlGeneratorChunkStepBuilder<CampaignDim, CampaignDim> {

    public CampaignDimChunk() {
        super(CampaignDim.class, CampaignDim.class, "campaign_dim_chunk");
    }
}
