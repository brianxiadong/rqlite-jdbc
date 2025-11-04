package io.rqlite;

import io.rqlite.client.L4Statement;
import j8spec.annotation.DefinedOrder;
import j8spec.junit.J8SpecRunner;
import org.junit.runner.RunWith;
import io.rqlite.json.JsonArray;

import static j8spec.J8Spec.*;
import static org.junit.Assert.*;

@DefinedOrder
@RunWith(J8SpecRunner.class)
public class L4StatementTest {
  static {
    it("Creates rqlite prepared statements", () -> {
      // Simple statement with no parameters
      L4Statement builder1 = new L4Statement().sql("SELECT * FROM users");
      JsonArray statement1 = builder1.build();
      assertEquals("[\"SELECT * FROM users\"]", statement1.toString());

      // Statement with positional parameters
      L4Statement builder2 = new L4Statement()
        .sql("SELECT * FROM users WHERE id = ? AND name = ?")
        .withPositionalParam(1)
        .withPositionalParam("Alice");
      JsonArray statement2 = builder2.build();
      assertEquals("[\"SELECT * FROM users WHERE id = ? AND name = ?\",1,\"Alice\"]", statement2.toString());

      // Statement with named parameters
      L4Statement builder3 = new L4Statement()
        .sql("SELECT * FROM users WHERE id = :id AND name = :name")
        .withNamedParam("id", 1)
        .withNamedParam("name", "Alice");
      JsonArray statement3 = builder3.build();
      assertEquals("[\"SELECT * FROM users WHERE id = :id AND name = :name\",{\"id\":1,\"name\":\"Alice\"}]", statement3.toString());

      // Statement with a BLOB parameter
      byte[] blobData = new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
      L4Statement builder4 = new L4Statement()
        .sql("INSERT INTO users (id, data) VALUES (?, ?)")
        .withPositionalParam(1)
        .withPositionalParam(blobData);
      JsonArray statement4 = builder4.build();
      assertEquals("[\"INSERT INTO users (id, data) VALUES (?, ?)\",1,\"3q2+7w==\"]", statement4.toString());
    });
  }
}
