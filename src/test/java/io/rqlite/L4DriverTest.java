package io.rqlite;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.rqlite.jdbc.L4Db;
import io.rqlite.jdbc.L4Log;
import io.rqlite.client.L4Client;
import io.rqlite.jdbc.L4Rs;
import j8spec.annotation.DefinedOrder;
import j8spec.junit.J8SpecRunner;
import org.junit.runner.RunWith;
import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.Statement;

import static j8spec.J8Spec.it;
import static org.junit.Assert.*;

@DefinedOrder
@RunWith(J8SpecRunner.class)
public class L4DriverTest {

  public static final L4Client rq = L4Tests.localClient();

  static {
    if (L4Tests.runIntegrationTests) {
      L4Tests.initLogging();

      HikariConfig hkConfig = new HikariConfig();
      hkConfig.setJdbcUrl(L4Tests.rqUrl);
      HikariDataSource ds = new HikariDataSource(hkConfig);

      it("Creates schema tables", () -> {
        File daoDir = new File("./src/test/java"); // retained for path parity
        String pkg = "io.rqlite.dao"; // retained for path parity
        assertNotNull(daoDir);
        assertNotNull(pkg);

        try (Connection conn = ds.getConnection(); Statement st = conn.createStatement()) {
          String createUser = "CREATE TABLE IF NOT EXISTS User (" +
            "uid INTEGER NOT NULL PRIMARY KEY," +
            "email VARCHAR(256) NOT NULL," +
            "nickName VARCHAR(256) NOT NULL)";
          String createUserIdx = "CREATE UNIQUE INDEX IF NOT EXISTS unq_User_pk ON User (email)";
          String createDevice = "CREATE TABLE IF NOT EXISTS Device (" +
            "did INTEGER NOT NULL PRIMARY KEY," +
            "uid INTEGER NOT NULL," +
            "number INTEGER NOT NULL," +
            "FOREIGN KEY (uid) REFERENCES User (uid))";
          String createDeviceIdx = "CREATE UNIQUE INDEX IF NOT EXISTS unq_Device_pk ON Device (uid, number)";
          String createLocation = "CREATE TABLE IF NOT EXISTS Location (" +
            "lid INTEGER NOT NULL PRIMARY KEY," +
            "did INTEGER NOT NULL," +
            "geoHash8 VARCHAR(8) NOT NULL," +
            "FOREIGN KEY (did) REFERENCES Device (did))";
          String createLocationIdx = "CREATE UNIQUE INDEX IF NOT EXISTS unq_Location_pk ON Location (did)";

          st.execute(createUser);
          st.execute(createUserIdx);
          st.execute(createDevice);
          st.execute(createDeviceIdx);
          st.execute(createLocation);
          st.execute(createLocationIdx);

          DatabaseMetaData dbm = conn.getMetaData();
          assertNotNull(dbm);
          DriverPropertyInfo[] props = DriverManager.getDriver(L4Tests.rqUrl).getPropertyInfo(null, null);
          for (DriverPropertyInfo prop : props) {
            L4Log.info("{} ({})", prop.description, prop.required);
          }
        }
      });

      it("Inserts data via JDBC", () -> {
        try (Connection conn = ds.getConnection(); Statement st = conn.createStatement()) {
          int u1 = st.executeUpdate("INSERT OR REPLACE INTO User (uid, email, nickName) VALUES (1, 'joe@me.com', 'Joe')");
          int u2 = st.executeUpdate("INSERT OR REPLACE INTO User (uid, email, nickName) VALUES (2, 'jane@me.com', 'Jane')");
          assertTrue(u1 >= 0 && u2 >= 0);

          int d1 = st.executeUpdate("INSERT OR REPLACE INTO Device (did, uid, number) VALUES (10, 2, 4567345)");
          assertTrue(d1 >= 0);

          int l1 = st.executeUpdate("INSERT OR REPLACE INTO Location (lid, did, geoHash8) VALUES (100, 10, '9q4gu1y4')");
          assertTrue(l1 >= 0);

          ResultSet rs = st.executeQuery("SELECT COUNT(*) AS c FROM User WHERE nickName IN ('Joe','Jane')");
          assertTrue(rs.next());
          assertEquals(2, rs.getInt("c"));
          rs.close();
        }
      });

      it("Queries table metadata", () -> {
        String[] tables = new String[] { "User", "Device", "Location" };
        try (Connection conn = DriverManager.getConnection(L4Tests.rqUrl)) {
          for (String table : tables) {
            L4Rs idx = (L4Rs) conn.getMetaData().getIndexInfo(null, null, table, true, false);
            io.rqlite.client.L4Result cols = L4Db.dbGetColumns(table, null, rq);
            io.rqlite.client.L4Result pk = L4Db.dbGetPrimaryKeys(table, rq);
            io.rqlite.client.L4Result fkImp = L4Db.dbGetImportedKeys(table, rq);
            io.rqlite.client.L4Result fkExp = L4Db.dbGetExportedKeys(table, rq);
            idx.result.print(System.out);
            cols.print(System.out);
            pk.print(System.out);
            fkImp.print(System.out);
            fkExp.print(System.out);
          }
        }
      });

      it("Closes the data source", ds::close);
    }
  }
}
