package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.domain.ReasonNotBuyDim;

@Component("reason_not_buy_dim_chunk")
public class ReasonNotBuyDimChunk extends SqlGeneratorChunkStepBuilder<ReasonNotBuyDim, ReasonNotBuyDim> {
    public ReasonNotBuyDimChunk() {
        super(ReasonNotBuyDim.class, ReasonNotBuyDim.class, "reason_not_buy_dim_chunk");
    }
}
