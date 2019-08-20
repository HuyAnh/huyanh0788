package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.domain.ChannelAdDim;

@Component("channel_ad_dim_chunk")
public class ChannelAdDimChunk extends SqlGeneratorChunkStepBuilder<ChannelAdDim, ChannelAdDim> {
    public ChannelAdDimChunk() {
        super(ChannelAdDim.class, ChannelAdDim.class, "channel_ad_dim_chunk");
    }
}
