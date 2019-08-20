package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.PrimaryKey;

import java.sql.Timestamp;

@DWTable("combo_dim")
@Data
public class ComboDim {
    @PrimaryKey
    @DWTableColumn("combo_id")
    private String comboId;

    @DWTableColumn("combo_code")
    private String comboCode;

    @DWTableColumn("combo_name")
    private String comboName;

    @DWTableColumn("price")
    private Double price;

    @DWTableColumn("start_date")
    private Timestamp startDate;

    @DWTableColumn("end_date")
    private Timestamp endDate;
}
