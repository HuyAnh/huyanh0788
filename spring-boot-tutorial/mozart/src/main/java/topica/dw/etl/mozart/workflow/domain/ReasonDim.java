package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.PrimaryKey;

@Data
@DWTable("reason_dim")
public class ReasonDim {

    @PrimaryKey
    @DWTableColumn("reason_id")
    private String reasonId;

    @DWTableColumn("reason_name")
    private String reasonName;

    @DWTableColumn("group_reason_id")
    private Integer groupReasonId;

    @DWTableColumn("group_reason_name")
    private String groupReasonName;
}
