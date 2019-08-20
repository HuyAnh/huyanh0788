package topica.dw.etl.mozart.workflow.reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class JdbcTableColumnProcessor implements ItemProcessor<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcTableColumnProcessor.class);

    @Override
    public String process(String item) throws Exception {
        LOG.info("Json item:" + item);
        return item;
    }

}
