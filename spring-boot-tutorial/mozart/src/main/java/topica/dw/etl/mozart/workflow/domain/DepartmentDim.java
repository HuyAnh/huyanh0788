package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.PrimaryKey;

import java.io.Serializable;

@DWTable("department_dim")
@Data
public class DepartmentDim implements Serializable {
    @PrimaryKey
    @DWTableColumn("sub_department_id")
    private String subDepartmentId;

    @DWTableColumn("sub_department_name")
    private String subDepartmentName;

    @DWTableColumn("department_id")
    private Integer departmentId;

    @DWTableColumn("department_name")
    private String departmentName;

    @DWTableColumn("type")
    private String type;
}
