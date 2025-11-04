package io.rqlite;

import io.rqlite.jdbc.L4DbMeta;
import j8spec.annotation.DefinedOrder;
import j8spec.junit.J8SpecRunner;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static j8spec.J8Spec.it;
import static org.junit.Assert.*;

@DefinedOrder
@RunWith(J8SpecRunner.class)
public class L4ExposedTest {

  static {
    if (L4Tests.runIntegrationTests) {
      it("Interacts with rqlite via JDBC", () -> {
        L4Tests.initLogging();
        L4DbMeta.setDriverName("SQLite JDBC");

        Connection conn = DriverManager.getConnection(L4Tests.rqUrl);
        try {
          Statement st = conn.createStatement();
          try {
            // Ensure clean state for deterministic assertions
            st.execute("DROP TABLE IF EXISTS UsersKt");
            st.execute("CREATE TABLE UsersKt (id INTEGER, name VARCHAR(50))");

            st.executeUpdate("INSERT INTO UsersKt (id, name) VALUES (0, 'Alice')");
            st.executeUpdate("INSERT INTO UsersKt (id, name) VALUES (1, 'bob')");

            ResultSet rs = st.executeQuery("SELECT COUNT(*) AS c FROM UsersKt");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("c"));
            rs.close();
          } finally {
            st.close();
          }
        } finally {
          conn.close();
        }
      });
    }
  }
}
