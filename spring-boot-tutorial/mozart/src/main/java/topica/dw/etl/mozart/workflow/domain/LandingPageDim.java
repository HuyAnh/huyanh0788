package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.PrimaryKey;

@DWTable("landing_page_dim")
@Data
public class LandingPageDim {
    @PrimaryKey
    @DWTableColumn("landing_page_id")
    private Long landingPageId;

    @DWTableColumn("landing_page_name")
    private String landingPageName;

    @DWTableColumn("domain_id")
    private Long domainId;

    @DWTableColumn("domain_name")
    private String domainName;

    @DWTableColumn("domain_status")
    private String domainStatus;
}
