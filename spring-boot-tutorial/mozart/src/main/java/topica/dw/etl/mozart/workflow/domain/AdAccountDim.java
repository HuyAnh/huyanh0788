package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.PrimaryKey;

@DWTable("ad_account_dim")
@Data
public class AdAccountDim {

    @PrimaryKey
    @DWTableColumn("ad_account_id")
    private Integer adAccountId;

    @DWTableColumn("ad_account_name")
    private String adAccountName;

    @DWTableColumn("ad_account_status")
    private String adAccountStatus;

    @DWTableColumn("provider_id")
    private Integer providerId;

    @DWTableColumn("provider_name")
    private String providerName;
}
