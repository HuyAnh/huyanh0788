package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.PrimaryKey;

@DWTable("partner_dim")
@Data
public class PartnerDim {
    @PrimaryKey
    @DWTableColumn("partner_id")
    private String partnerId;

    @DWTableColumn("partner_name")
    private String partnerName;

    @DWTableColumn("partner_status")
    private String partnerStatus;

    @DWTableColumn("partner_code")
    private String partnerCode;

    @DWTableColumn("partner_type")
    private String partnerType;
}
