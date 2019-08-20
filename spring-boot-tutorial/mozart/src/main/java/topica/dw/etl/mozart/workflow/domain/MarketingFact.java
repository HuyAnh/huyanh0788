package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import lombok.EqualsAndHashCode;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;

import java.math.BigDecimal;
import java.math.BigInteger;

@Data
@DWTable("marketing_fact")
@EqualsAndHashCode(callSuper = true)
public class MarketingFact extends BaseDim{

    @DWTableColumn("date_c3_id")
    private Integer dateC3Id;

    @DWTableColumn("date_lx_id")
    private Integer dateLxId;

    @DWTableColumn("mkter_id")
    private Integer mkterId;

    @DWTableColumn("sub_department_mkter_id")
    private String subDepartmentMkterId;

    @DWTableColumn("course_id")
    private String courseId;

    @DWTableColumn("combo_code")
    private String comboCode;

    @DWTableColumn("target")
    private String target;

    @DWTableColumn("country_id")
    private Integer countryId;

    @DWTableColumn("ad_id")
    private Integer adId;

    @DWTableColumn("channel_ad_id")
    private Integer channelAdId;

    @DWTableColumn("ad_account_id")
    private Integer adAccountId;

    @DWTableColumn("campaign_id")
    private Integer campaignId;

    @DWTableColumn("landing_page_id")
    private BigInteger landingPageId;

    @DWTableColumn("source_contactc3_code")
    private String sourceContactc3Code;

    @DWTableColumn("number_c1")
    private BigDecimal numberC1;

    @DWTableColumn("number_c2")
    private BigDecimal numberC2;

    @DWTableColumn("cost")
    private BigDecimal cost;

    @DWTableColumn("number_c3")
    private BigDecimal numberC3;

    @DWTableColumn("number_c3_c")
    private BigDecimal numberC3C;

    @DWTableColumn("number_l1")
    private BigDecimal numberL1;

    @DWTableColumn("number_l1_c")
    private BigDecimal numberL1C;

    @DWTableColumn("number_l2")
    private BigDecimal numberL2;

    @DWTableColumn("number_l2_c")
    private BigDecimal numberL2C;

    @DWTableColumn("number_l3")
    private BigDecimal numberL3;

    @DWTableColumn("number_l3_c")
    private BigDecimal numberL3C;

    @DWTableColumn("number_l4")
    private BigDecimal numberL4;

    @DWTableColumn("number_l4_c")
    private BigDecimal numberL4C;

    @DWTableColumn("number_l7")
    private BigDecimal numberL7;

    @DWTableColumn("number_l7_c")
    private BigDecimal numberL7C;

    @DWTableColumn("number_l8")
    private BigDecimal numberL8;

    @DWTableColumn("number_l8_c")
    private BigDecimal numberL8C;

    @DWTableColumn("revenue_before_control")
    private BigDecimal revenueBeforeControl;

    @DWTableColumn("revenue_before_control_c")
    private BigDecimal revenueBeforeControlC;

    @DWTableColumn("revenue_after_control")
    private BigDecimal revenueAfterControl;

}
