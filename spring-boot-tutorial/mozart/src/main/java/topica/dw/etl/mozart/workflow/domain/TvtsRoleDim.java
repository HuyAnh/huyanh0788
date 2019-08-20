package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.PrimaryKey;

@DWTable("tvts_role_dim")
@Data
public class TvtsRoleDim {
    @PrimaryKey
    @DWTableColumn("tvts_role_code")
    private String tvtsRoleCode;

    @DWTableColumn("tvts_role_name")
    private String tvtsRoleName;
}
