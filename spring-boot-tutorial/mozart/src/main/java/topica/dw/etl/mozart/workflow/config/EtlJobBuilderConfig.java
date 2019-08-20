package topica.dw.etl.mozart.workflow.config;

import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import topica.dw.etl.mozart.workflow.domain.*;
import topica.dw.etl.mozart.workflow.job.EtlJobBuilder;
import topica.dw.etl.mozart.workflow.job.chunk.*;
import topica.dw.etl.mozart.workflow.job.tasklet.SingleTaskletJobBuilder;

@Configuration
@ComponentScan("topica.dw.etl.mozart.workflow.config")
public class EtlJobBuilderConfig {

    // Spring batch
    @Autowired
    private JobBuilderFactory jobFactory;

    @Autowired
    private StepBuilderFactory stepFactory;

    // Builder bean
    @Autowired
    @Qualifier("course_dim_chunk")
    private ChunkStepBuilder<CourseDim, CourseDim> courseDimChunk;

    @Autowired
    @Qualifier("mkter_dim_chunk")
    private ChunkStepBuilder<MkterDim, MkterDim> mkterDimChunk;

    @Autowired
    @Qualifier("staff_dim_chunk")
    private ChunkStepBuilder<StaffDim, StaffDim> staffDimChunk;

    @Autowired
    @Qualifier("department_dim_chunk")
    private ChunkStepBuilder<DepartmentDim, DepartmentDim> departmentDimChunk;

    @Autowired
    @Qualifier("ad_dim_chunk")
    private ChunkStepBuilder<AdDim, AdDim> adDimChunk;

    @Autowired
    @Qualifier("combo_dim_chunk")
    private ChunkStepBuilder<ComboDim, ComboDim> comboDimChunk;

    @Autowired
    @Qualifier("channel_ad_dim_chunk")
    private ChunkStepBuilder<ChannelAdDim, ChannelAdDim> channelAdDimChunk;

    @Autowired
    @Qualifier("landing_page_dim_chunk")
    private ChunkStepBuilder<LandingPageDim, LandingPageDim> landingPageDimChunk;

    @Autowired
    @Qualifier("partner_dim_chunk")
    private ChunkStepBuilder<PartnerDim, PartnerDim> partnerDimChunk;

    @Autowired
    @Qualifier("reason_not_buy_dim_chunk")
    private ChunkStepBuilder<ReasonNotBuyDim, ReasonNotBuyDim> reasonNotBuyDimChunk;

    @Autowired
    @Qualifier("ad_account_dim_chunk")
    private ChunkStepBuilder<AdAccountDim, AdAccountDim> adAccountDimChunk;

    @Autowired
    @Qualifier("campaign_dim_chunk")
    private ChunkStepBuilder<CampaignDim, CampaignDim> campaignDimChunk;

    @Autowired
    @Qualifier("area_dim_chunk")
    private ChunkStepBuilder<AreaDim, AreaDim> areaDimChunk;

    @Autowired
    @Qualifier("sale_fact_chunk")
    private ChunkStepBuilder<SaleFact, SaleFact> saleFactChunk;

    @Autowired
    @Qualifier("reason_dim_chunk")
    private ChunkStepBuilder<ReasonDim, ReasonDim> reasonDimChunk;

    @Autowired
    @Qualifier("tvts_role_dim_chunk")
    private ChunkStepBuilder<TvtsRoleDim, TvtsRoleDim> tvtsRoleDimChunk;

    @Autowired
    @Qualifier("marketing_fact_chunk")
    private ChunkStepBuilder<MarketingFact, MarketingFact> marketingFactChunk;

    @Autowired
    @Qualifier("start_time_tasklet")
    private Tasklet startTimeTasklet;

    @Autowired
    @Qualifier("end_time_tasklet")
    private Tasklet endTimeTasklet;

    @Autowired
    @Qualifier("edumall_contactc3_tasklet")
    private Tasklet edumallContactc3Tasklet;

    @Autowired
    @Qualifier("delete_sale_fact_by_date_c3_id_tasklet")
    private Tasklet deleteSaleFactByDateC3IdTasklet;

    @Autowired
    @Qualifier("bifrost_nami_or_bifrost_sale_detail_tasklet")
    private Tasklet bifrostNamiOrBifrostSaleDetailTasklet;

    @Autowired
    @Qualifier("minerva_c3_tele_sale_detail_tasklet")
    private Tasklet minervaC3TeleSaleDetailTasklet;

    @Autowired
    @Qualifier("minerva_not_c3_tele_sale_detail_tasklet")
    private Tasklet minervaNotC3TeleSaleDetailTasklet;

    @Autowired
    @Qualifier("no_course_bifrost_nami_or_bifrost_sale_detail_tasklet")
    private Tasklet noCourseBifrostNamiOrBifrostSaleDetailTasklet;

    @Autowired
    @Qualifier("no_course_minerva_c3_tele_sale_detail_tasklet")
    private Tasklet noCourseMinervaC3TeleSaleDetailTasklet;

    @Autowired
    @Qualifier("no_course_minerva_not_c3_tele_sale_detail_tasklet")
    private Tasklet noCourseMinervaNotC3TeleSaleDetailTasklet;

    @Autowired
    @Qualifier("delete_marketing_fact_by_date_c3_id_tasklet")
    private Tasklet deleteMarketingFactByDateC3IdTasklet;

    @Autowired
    @Qualifier("upsert_ranked_history_orders_tasklet")
    private Tasklet upsertRankedHistoryOrdersTasklet;

    @Autowired
    @Qualifier("upsert_ranked_bifrost_transactions_tasklet")
    private Tasklet upsertRankedBifrostTransactionsTasklet;

    @Autowired
    @Qualifier("upsert_ranked_gambit_orders_tasklet")
    private Tasklet upsertRankedGambitOrdersTasklet;

    @Autowired
    @Qualifier("insert_log_mozart_transformation_tasklet")
    private Tasklet insertLogMozartTransformationTasklet;

    @Autowired
    @Qualifier("upsert_edumall_contactc3s_from_bifrost_transactions_tasklet")
    private Tasklet upsertEdumallContactc3sFromBifrostTransactionsTasklet;

    @Autowired
    @Qualifier("upsert_edumall_contactc3s_from_eros_tasklet")
    private Tasklet upsertEdumallContactc3sFromErosTasklet;

    @Autowired
    @Qualifier("upsert_edumall_contactc3s_from_minerva_tasklet")
    private Tasklet upsertEdumallContactc3sFromMinervaTasklet;

    @Autowired
    @Qualifier("upsert_edumall_contactc3s_from_nami_tasklet")
    private Tasklet upsertEdumallContactc3sFromNamiTasklet;

    @Bean(name = "course_dim_chunk_builder")
    public EtlJobBuilder buildCourseDimChunk() {
        return new ChunkJobBuilder("course_dim_chunk", jobFactory, courseDimChunk);
    }

    @Bean(name = "mkter_dim_chunk_builder")
    public EtlJobBuilder buildMkterDimChunk() {
        return new ChunkJobBuilder("mkter_dim_chunk", jobFactory, mkterDimChunk);
    }

    @Bean(name = "combo_dim_chunk_builder")
    public EtlJobBuilder buildComboDimChunk() {
        return new ChunkJobBuilder("combo_dim_chunk", jobFactory, comboDimChunk);
    }

    @Bean(name = "staff_dim_chunk_builder")
    public EtlJobBuilder buildStaffDimChunk() {
        return new ChunkJobBuilder("staff_dim_chunk", jobFactory, staffDimChunk);
    }

    @Bean(name = "department_dim_chunk_builder")
    public EtlJobBuilder buildDepartmentDimChunk() {
        return new ChunkJobBuilder("department_dim_chunk", jobFactory, departmentDimChunk);
    }

    @Bean(name = "ad_account_dim_chunk_builder")
    public EtlJobBuilder buildAdAccountDimChunk() {
        return new ChunkJobBuilder("ad_account_dim_chunk", jobFactory, adAccountDimChunk);
    }

    @Bean(name = "campaign_dim_chunk_builder")
    public EtlJobBuilder buildCampaignDimChunk() {
        return new ChunkJobBuilder("campaign_dim_chunk", jobFactory, campaignDimChunk);
    }

    @Bean(name = "area_dim_chunk_builder")
    public EtlJobBuilder buildAreaDimChunk() {
        return new ChunkJobBuilder("area_dim_chunk", jobFactory, areaDimChunk);
    }

    @Bean(name = "sale_fact_chunk_builder")
    public EtlJobBuilder buildSaleFactChunk() {
        return new ChunkJobBuilder("sale_fact_chunk", jobFactory, saleFactChunk);
    }

    @Bean(name = "tvts_role_dim_chunk_job_builder")
    public EtlJobBuilder buildTvtsRoleDimChunk() {
        return new ChunkJobBuilder("tvts_role_dim_chunk", jobFactory, tvtsRoleDimChunk);
    }

    @Bean(name = "ad_dim_chunk_job_builder")
    public EtlJobBuilder buildAdDimChunk() {
        return new ChunkJobBuilder("ad_dim_chunk", jobFactory, adDimChunk);
    }

    @Bean(name = "channel_ad_dim_chunk_job_builder")
    public EtlJobBuilder buildChannelAdDimChunk() {
        return new ChunkJobBuilder("channel_ad_dim_chunk", jobFactory, channelAdDimChunk);
    }

    @Bean(name = "landing_page_dim_chunk_job_builder")
    public EtlJobBuilder buildLandingPageDimChunk() {
        return new ChunkJobBuilder("landing_page_dim_chunk", jobFactory, landingPageDimChunk);
    }

    @Bean(name = "partner_dim_chunk_job_builder")
    public EtlJobBuilder buildPartnerDimChunk() {
        return new ChunkJobBuilder("partner_dim_chunk", jobFactory, partnerDimChunk);
    }

    @Bean(name = "reason_not_buy_dim_chunk_job_builder")
    public EtlJobBuilder buildReasonNotBuyDimChunk() {
        return new ChunkJobBuilder("reason_not_buy_dim_chunk", jobFactory, reasonNotBuyDimChunk);
    }

    @Bean(name = "reason_dim_chunk_job_builder")
    public EtlJobBuilder buildReasonDimChunk() {
        return new ChunkJobBuilder("reason_dim_chunk", jobFactory, reasonDimChunk);
    }

    @Bean(name = "start_time_tasklet_builder")
    public EtlJobBuilder buildStartTimeTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "start_time_tasklet", startTimeTasklet);
    }

    @Bean(name = "end_time_tasklet_builder")
    public EtlJobBuilder buildEndTimeTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "end_time_tasklet", endTimeTasklet);
    }

    @Bean(name = "edumall_contactc3_tasklet_builder")
    public EtlJobBuilder buildEdumallContactC3Tasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "edumall_contactc3_tasklet",
                edumallContactc3Tasklet);
    }

    @Bean(name = "delete_sale_fact_by_date_c3_id_tasklet_builder")
    public EtlJobBuilder buildDeleteSaleFactByDateC3IdTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "delete_sale_fact_by_date_c3_id_tasklet",
                deleteSaleFactByDateC3IdTasklet);
    }

    @Bean(name = "bifrost_nami_or_bifrost_sale_detail_builder")
    public EtlJobBuilder buildBifrostNamiOrNamiSaleDetailTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "bifrost_nami_or_bifrost_sale_detail_tasklet", bifrostNamiOrBifrostSaleDetailTasklet);
    }

    @Bean(name = "minerva_c3_tele_sale_detail_builder")
    public EtlJobBuilder buildMinervaC3TeleSaleDetailTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "minerva_c3_tele_sale_detail_tasklet", minervaC3TeleSaleDetailTasklet);
    }

    @Bean(name = "minerva_not_c3_tele_sale_detail_builder")
    public EtlJobBuilder buildMinervaNotC3TeleSaleDetailTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "minerva_not_c3_tele_sale_detail_tasklet", minervaNotC3TeleSaleDetailTasklet);
    }

    @Bean(name = "no_course_bifrost_nami_or_bifrost_sale_detail_builder")
    public EtlJobBuilder buildNoCourseBifrostNamiOrNamiSaleDetailTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "no_course_bifrost_nami_or_bifrost_sale_detail_tasklet", noCourseBifrostNamiOrBifrostSaleDetailTasklet);
    }

    @Bean(name = "no_course_minerva_c3_tele_sale_detail_builder")
    public EtlJobBuilder buildNoCourseMinervaC3TeleSaleDetailTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "no_course_minerva_c3_tele_sale_detail_tasklet", noCourseMinervaC3TeleSaleDetailTasklet);
    }

    @Bean(name = "no_course_minerva_not_c3_tele_sale_detail_builder")
    public EtlJobBuilder buildNoCourseMinervaNotC3TeleSaleDetailTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "no_course_minerva_not_c3_tele_sale_detail_tasklet", noCourseMinervaNotC3TeleSaleDetailTasklet);
    }

    @Bean(name = "delete_marketing_fact_by_date_c3_id_tasklet_builder")
    public EtlJobBuilder buildDeleteMarketingFactByDateC3IdTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "delete_marketing_fact_by_date_c3_id_tasklet",
                deleteMarketingFactByDateC3IdTasklet);
    }

    @Bean(name = "marketing_fact_chunk_builder")
    public EtlJobBuilder buildMarketingFactChunk() {
        return new ChunkJobBuilder("marketing_fact_chunk", jobFactory, marketingFactChunk);
    }

    @Bean(name = "upsert_ranked_history_orders_tasklet_builder")
    public EtlJobBuilder buildUpsertRankedHistoryOrdersTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "upsert_ranked_history_orders_tasklet", upsertRankedHistoryOrdersTasklet);
    }

    @Bean(name = "upsert_ranked_bifrost_transactions_tasklet_builder")
    public EtlJobBuilder buildUpsertRankedBifrostTransactionsTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "upsert_ranked_bifrost_transactions_tasklet", upsertRankedBifrostTransactionsTasklet);
    }

    @Bean(name = "upsert_ranked_gambit_orders_tasklet_builder")
    public EtlJobBuilder buildUpsertRankedGambitOrdersTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "upsert_ranked_gambit_orders_tasklet", upsertRankedGambitOrdersTasklet);
    }

    @Bean(name = "insert_log_mozart_transformation_tasklet_builder")
    public EtlJobBuilder buildInsertLogMozartTransformationTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "insert_log_mozart_transformation_tasklet", insertLogMozartTransformationTasklet);
    }

    @Bean(name = "upsert_edumall_contactc3s_from_bifrost_transactions_tasklet_builder")
    public EtlJobBuilder buildUpsertEdumallContactc3sFromBifrostTransactionsTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "upsert_edumall_contactc3s_from_bifrost_transactions_tasklet", upsertEdumallContactc3sFromBifrostTransactionsTasklet);
    }

    @Bean(name = "upsert_edumall_contactc3s_from_eros_tasklet_builder")
    public EtlJobBuilder buildUpsertEdumallContactc3sFromErosTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "upsert_edumall_contactc3s_from_eros_tasklet", upsertEdumallContactc3sFromErosTasklet);
    }

    @Bean(name = "upsert_edumall_contactc3s_from_minerva_tasklet_builder")
    public EtlJobBuilder buildUpsertEdumallContactc3sFromMinervaTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "upsert_edumall_contactc3s_from_minerva_tasklet", upsertEdumallContactc3sFromMinervaTasklet);
    }

    @Bean(name = "upsert_edumall_contactc3s_from_nami_tasklet_builder")
    public EtlJobBuilder buildUpsertEdumallContactc3sFromNamiTasklet() {
        return new SingleTaskletJobBuilder(jobFactory, stepFactory, "upsert_edumall_contactc3s_from_nami_tasklet", upsertEdumallContactc3sFromNamiTasklet);
    }
}
