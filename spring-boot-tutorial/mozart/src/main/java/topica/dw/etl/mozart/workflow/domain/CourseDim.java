package topica.dw.etl.mozart.workflow.domain;

import lombok.Data;
import topica.dw.etl.mozart.workflow.common.annotation.DWTable;
import topica.dw.etl.mozart.workflow.common.annotation.DWTableColumn;
import topica.dw.etl.mozart.workflow.common.annotation.PrimaryKey;

import java.io.Serializable;

@DWTable("course_dim")
@Data
public class CourseDim implements Serializable {
    @PrimaryKey
    @DWTableColumn("course_id")
    private String courseId;

    @DWTableColumn("course_name")
    private String courseName;

    @DWTableColumn("course_version")
    private String courseVersion;

    @DWTableColumn("teacher_id")
    private String teacherId;

    @DWTableColumn("teacher_username")
    private String teacherUsername;

    @DWTableColumn("teacher_email")
    private String teacherEmail;

    @DWTableColumn("teacher_name")
    private String teacherName;

    @DWTableColumn("sub_category_id")
    private String subCategoryId;

    @DWTableColumn("sub_category_name")
    private String subCategoryName;

    @DWTableColumn("sub_category_enabled")
    private Boolean subCategoryEnabled;

    @DWTableColumn("category_id")
    private String categoryId;

    @DWTableColumn("category_name")
    private String categoryName;

    @DWTableColumn("category_enabled")
    private Boolean categoryEnabled;
}
