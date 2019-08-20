package topica.dw.etl.mozart.workflow.job.chunk;

import org.springframework.stereotype.Component;
import topica.dw.etl.mozart.workflow.domain.TvtsRoleDim;

@Component("tvts_role_dim_chunk")
public class TvtsRoleDimChunk extends SqlGeneratorChunkStepBuilder<TvtsRoleDim, TvtsRoleDim> {
    public TvtsRoleDimChunk() {
        super(TvtsRoleDim.class, TvtsRoleDim.class, "tvts_role_dim_chunk");
    }
}
