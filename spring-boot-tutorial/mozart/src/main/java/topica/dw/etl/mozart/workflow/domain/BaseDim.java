package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;

import java.sql.Timestamp;
import java.time.Instant;

@Data
public class BaseDim {

	@DWTableColumn(value = "updated_at", ignoreAssign = true)
	private Timestamp updatedAt = new Timestamp(Instant.now().toEpochMilli());

}
