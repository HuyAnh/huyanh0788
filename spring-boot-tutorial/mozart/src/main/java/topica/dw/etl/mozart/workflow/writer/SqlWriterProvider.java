package topica.dw.etl.mozart.workflow.writer;

import topica.dw.etl.mozart.workflow.common.exception.UnsupportedGenerateSqlException;

public interface SqlWriterProvider<T> {
    final String INSERT_QUERY = "INSERT INTO ";
    final String LEFT_PARENTHESIS = "(";
    final String RIGHT_PARENTHESIS = ")";
    final String COLON = ":";
    final String VALUES = "VALUES";
    final String SPACE = " ";
    final String SEMICOLON = ";";
    final String EQUAL = "=";

    String writeSql() throws UnsupportedGenerateSqlException;
}
