package topica.dw.etl.mozart.workflow.writer;

import topica.dw.etl.mozart.workflow.common.exception.UnsupportedGenerateSqlException;
import topica.dw.etl.mozart.workflow.writer.provider.mysql.MySqlAnnotationPropertyWriterProvider;
import topica.dw.etl.mozart.workflow.writer.provider.postgresql.PostgreSqlAnnotationPropertyWriterProvider;

import java.util.function.Function;

public interface DwWriterProviderFactory {

    enum WriterProvider {
        MYSQL("mysql", ProviderType.RDBMS), POSTGRESQL("postgresql", ProviderType.RDBMS);
        private String name;
        private ProviderType type;

        WriterProvider(String name, ProviderType type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public ProviderType getType() {
            return type;
        }

        @Override
        public String toString() {
            return getName();
        }

        public static WriterProvider findByName(String name) {
            for (WriterProvider provider : WriterProvider.values()) {
                if (provider.getName().equalsIgnoreCase(name))
                    return provider;
            }
            return null;
        }

    }

    enum ProviderType {
        RDBMS, NOSQL;

        ProviderType() {

        }
    }

    public default <T> Function<Class<T>, SqlWriterProvider<T>> buildWriterProvider(WriterProvider provider) {
        return t -> {
            switch (provider) {
                case MYSQL:
                    return new MySqlAnnotationPropertyWriterProvider<T>() {
                        @Override
                        public String writeSql() throws UnsupportedGenerateSqlException {
                            return extractSqlFromThis(t);
                        }
                    };
                case POSTGRESQL:
                    return new PostgreSqlAnnotationPropertyWriterProvider<T>() {
                        @Override
                        public String writeSql() throws UnsupportedGenerateSqlException {
                            return extractSqlFromThis(t);
                        }
                    };
                default:
                    return null;
            }
        };
    }
}
