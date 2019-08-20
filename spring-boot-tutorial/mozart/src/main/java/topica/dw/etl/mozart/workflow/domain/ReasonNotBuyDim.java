package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.PrimaryKey;

@DWTable("reason_not_buy_dim")
@Data
public class ReasonNotBuyDim {
    @PrimaryKey
    @DWTableColumn("reason_not_buy_id")
    private String reasonNotBuyId;

    @DWTableColumn("reason_not_buy_name")
    private String getReasonNotBuyName;
}
