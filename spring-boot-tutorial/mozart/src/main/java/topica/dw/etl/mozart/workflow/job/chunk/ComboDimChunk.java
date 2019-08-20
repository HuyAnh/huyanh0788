package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.domain.ComboDim;

@Component("combo_dim_chunk")
public class ComboDimChunk extends SqlGeneratorChunkStepBuilder<ComboDim, ComboDim> {
    public ComboDimChunk() {
        super(ComboDim.class, ComboDim.class, "combo_dim_chunk");
    }
}
