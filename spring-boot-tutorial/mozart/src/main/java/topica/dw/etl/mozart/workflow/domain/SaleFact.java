package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import lombok.EqualsAndHashCode;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;

import java.math.BigDecimal;

@DWTable("sale_fact")
@Data
@EqualsAndHashCode(callSuper = true)
public class SaleFact extends BaseDim{

    @DWTableColumn("date_c3_id")
    private Integer dateC3Id;

    @DWTableColumn("date_lx_id")
    private Integer dateLxId;

    @DWTableColumn("sub_department_sale_id")
    private String subDepartmentSaleId;

    @DWTableColumn("staff_id")
    private String staffId;

    @DWTableColumn("tvts_role_code")
    private String tvtsRoleCode;

    @DWTableColumn("sub_department_mkter_id")
    private String subDepartmentMkterId;

    @DWTableColumn("mkter_id")
    private Integer mkterId;

    @DWTableColumn("course_id")
    private String courseId;

    @DWTableColumn("payment_method_code")
    private String paymentMethodCode;

    @DWTableColumn("country_id")
    private Integer countryId;

    @DWTableColumn("payment_status_code")
    private String paymentStatusCode;

    @DWTableColumn("combo_code")
    private String comboCode;

    @DWTableColumn("target")
    private String target;

    @DWTableColumn("partner_id")
    private String partnerId;

    @DWTableColumn("channel_ad_id")
    private Integer channelAdId;

    @DWTableColumn("source_contactc3_code")
    private String sourceContactc3Code;

    @DWTableColumn("reason_not_buy_id")
    private String reasonNotBuyId;

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

    @DWTableColumn("number_call")
    private BigDecimal numberCall;

    @DWTableColumn("duration")
    private BigDecimal duration;

    @DWTableColumn("number_order_canceled")
    private BigDecimal numberOrderCanceled;

    @DWTableColumn("revenue_before_control")
    private BigDecimal revenueBeforeControl;

    @DWTableColumn("revenue_before_control_c")
    private BigDecimal revenueBeforeControlC;

    @DWTableColumn("revenue_after_control")
    private BigDecimal revenueAfterControl;

    @DWTableColumn("sale_method_id")
    private Integer saleMethodId;
}