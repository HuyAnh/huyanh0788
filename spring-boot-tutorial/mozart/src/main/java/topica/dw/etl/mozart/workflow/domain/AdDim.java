package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.PrimaryKey;

@DWTable("ad_dim")
@Data
public class AdDim {

    @PrimaryKey
    @DWTableColumn("ad_id")
    private Integer adId;

    @DWTableColumn("ad_name")
    private String adName;

    @DWTableColumn("utm_source")
    private String utmSource;

    @DWTableColumn("utm_medium")
    private String utmMedium;

    @DWTableColumn("utm_campaign")
    private String utmCampaign;

    @DWTableColumn("tracking_link")
    private String trackingLink;

    @DWTableColumn("status")
    private Integer status;

    @DWTableColumn("ad_group_id")
    private Integer adGroupId;

    @DWTableColumn("ad_group_name")
    private String adGroupName;

}
