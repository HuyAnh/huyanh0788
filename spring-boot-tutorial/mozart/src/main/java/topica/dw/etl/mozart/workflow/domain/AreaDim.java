package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.PrimaryKey;

@DWTable("area_dim")
@Data
public class AreaDim {

    @PrimaryKey
    @DWTableColumn("district_id")
    private String districtId;

    @DWTableColumn("district_code")
    private String districtCode;

    @DWTableColumn("district_name")
    private String districtName;

    @DWTableColumn("province_code")
    private String provinceCode;

    @DWTableColumn("provine_name")
    private String provineName;

    @DWTableColumn("region_name")
    private String regionName;

    @DWTableColumn("country_code")
    private String countryCode;

    @DWTableColumn("country_name")
    private String countryName;

}
