package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.PrimaryKey;

import java.io.Serializable;

@DWTable("staff_dim")
@Data
public class StaffDim implements Serializable {
    @PrimaryKey
    @DWTableColumn("staff_id")
    private String staffId;

    @DWTableColumn("staff_name")
    private String staffName;

    @DWTableColumn("staff_email")
    private String staffEmail;

    @DWTableColumn("staff_role")
    private String staffRole;
}