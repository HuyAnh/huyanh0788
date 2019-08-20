package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.PrimaryKey;

@DWTable("mkter_dim")
@Data
public class MkterDim {

    @PrimaryKey
    @DWTableColumn("mkter_id")
    private Integer mkterId;

    @DWTableColumn("mkter_name")
    private String mkterName;

    @DWTableColumn("mkter_email")
    private String mkterEmail;

    @DWTableColumn("mkter_role_code")
    private String mkterRoleCode;

    @DWTableColumn("mkter_role_name")
    private String mkterRoleName;
}
