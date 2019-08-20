package topica.dw.etl.mozart.workflow.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class IOCommon {

    private static final String SQL_DEFAULT_RESOURCE_PATH = "/etl/sql/";

    public static String readSqlFileInputStream(InputStream input) throws IOException {
        return readSqlStream(input, " ");
    }

    private static String readSqlStream(InputStream input, String joinLineCharactor) throws IOException {
        try (BufferedReader buffer = new BufferedReader(new InputStreamReader(input))) {
            return buffer.lines().collect(Collectors.joining(joinLineCharactor));
        }
    }

    private void printSql(String resource) throws Exception {
        System.out.println(readSqlFileInputStream(this.getClass().getResourceAsStream(resource)));
    }

    public static void main(String args[]) throws Exception {
        String sqlResource = SQL_DEFAULT_RESOURCE_PATH.concat("lading_date_dms.sql");
        IOCommon common = new IOCommon();
        common.printSql(sqlResource);
    }
}
