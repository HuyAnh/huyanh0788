package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.PrimaryKey;

@DWTable("channel_ad_dim")
@Data
public class ChannelAdDim {
    @PrimaryKey
    @DWTableColumn("channel_ad_id")
    private Integer channelAdId;

    @DWTableColumn("channel_ad_code")
    private String channelAdCode;

    @DWTableColumn("channel_ad_name")
    private String channelAdName;
}
