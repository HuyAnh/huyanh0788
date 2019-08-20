package topica.dw.etl.mozart.workflow.common.ulti;

import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class Helper {

    private Helper() {
    }

    public static String processListToString(List<String> stringList) {
        StringJoiner stringJoiner = new StringJoiner(", ", "(", ")");
        stringJoiner.add("-1");

        for (String s : stringList) {
            stringJoiner.add(s);
        }

        return stringJoiner.toString();
    }

    private static List<String> processResultSetToListString(ResultSet resultSet, String nameColumn) throws SQLException {
        List<String> result = new ArrayList<>();
        while (resultSet.next()){
            result.add(resultSet.getString(nameColumn));
        }
        return result;
    }

    public static List<String> buildReadSql(Statement statement, String sql, String nameColumn) throws SQLException {
        ResultSet resultSet = statement.executeQuery(sql);
        return processResultSetToListString(resultSet, nameColumn);
    }

    public static String buildSqlFact(Statement statement, String sqlFact, String sqlListDateC3IdChanged,
                                      String nameColumn, String regexReplace) throws SQLException {

        String dateC3Ids = Helper.processListToString(Helper.buildReadSql(statement, sqlListDateC3IdChanged, nameColumn));
        return StringUtils.replaceAll(sqlFact, regexReplace, dateC3Ids);

    }

}
