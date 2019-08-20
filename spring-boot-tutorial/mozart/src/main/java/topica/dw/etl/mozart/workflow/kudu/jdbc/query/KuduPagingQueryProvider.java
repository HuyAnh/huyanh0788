package topica.dw.etl.mozart.workflow.kudu.jdbc.query;

import org.springframework.batch.item.database.support.AbstractSqlPagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryUtils;
import org.springframework.util.StringUtils;

public class KuduPagingQueryProvider extends AbstractSqlPagingQueryProvider {

    @Override
    public String generateFirstPageQuery(int pageSize) {
        return SqlPagingQueryUtils.generateLimitSqlQuery(this, false, buildLimitClause(pageSize));
    }

    @Override
    public String generateRemainingPagesQuery(int pageSize) {
        if (StringUtils.hasText(getGroupClause())) {
            return SqlPagingQueryUtils.generateLimitGroupedSqlQuery(this, true, buildLimitClause(pageSize));
        } else {
            return SqlPagingQueryUtils.generateLimitSqlQuery(this, true, buildLimitClause(pageSize));
        }
    }

    private String buildLimitClause(int pageSize) {
        return new StringBuilder().append("LIMIT ").append(pageSize).toString();
    }

    @Override
    public String generateJumpToItemQuery(int itemIndex, int pageSize) {
        int page = itemIndex / pageSize;
        int offset = (page * pageSize) - 1;
        offset = offset < 0 ? 0 : offset;

        String limitClause = new StringBuilder().append("LIMIT ").append(offset).append(", 1").toString();
        return SqlPagingQueryUtils.generateLimitJumpToQuery(this, limitClause);
    }

}
