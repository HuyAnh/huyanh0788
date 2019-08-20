package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.domain.MkterDim;

@Component("mkter_dim_chunk")
public class MkterDimChunk extends SqlGeneratorChunkStepBuilder<MkterDim, MkterDim> {
    public MkterDimChunk() {
        super(MkterDim.class, MkterDim.class, "mkter_dim_chunk");
    }
}
