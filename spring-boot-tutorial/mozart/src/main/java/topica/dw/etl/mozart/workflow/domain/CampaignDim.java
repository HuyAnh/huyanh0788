package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.PrimaryKey;

@DWTable("campaign_dim")
@Data
public class CampaignDim {
    @PrimaryKey
    @DWTableColumn("campaign_id")
    private Integer campaignId;

    @DWTableColumn("campaign_name")
    private String campaignName;

    @DWTableColumn("campaign_status")
    private Integer campaignStatus;

    @DWTableColumn("campaign_status_name")
    private String campaignStatusName;

}
