package topica.dw.etl.mozart.workflow.jdbc;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.sql.DataSource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import topica.dw.etl.mozart.workflow.config.EdumallDwDatasourceConfig;

@RunWith(SpringRunner.class)
@Configuration
@ContextConfiguration(loader = AnnotationConfigContextLoader.class, classes = { EdumallDwDatasourceConfig.class })
@ActiveProfiles(profiles = "test")
public class TestInsertSymbolCharactor {

	@Configuration
	@PropertySource("classpath:application-test.properties")
	public static class MyContextConfiguration {

	}

	@Autowired
	@Qualifier("warehouseDS")
	private DataSource edumallDw;

	@Test
	public void testInsertSymbolCharactor() throws Exception {
		Connection conn = edumallDw.getConnection();
		assertTrue(!conn.isClosed());
		String query = "insert into Test_Symbol(id,symbol) values(1, 'Nguyễn Thành Trung 😄😃')";
		// String query = "insert into Test_Symbol(id,symbol) values(1, 'đổi
		// sang khóa này e ui')";
		Statement stmt = conn.createStatement();
		stmt.execute(query);

		stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("Select * from Test_Symbol");
		while (rs.next()) {
			System.out.println(rs.getInt(1) + "/" + rs.getString(2));
		}

	}
}
