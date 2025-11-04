package io.rqlite;

import io.rqlite.client.L4Statement;
import io.rqlite.client.L4Client;
import io.rqlite.client.L4Response;
import j8spec.annotation.DefinedOrder;
import j8spec.junit.J8SpecRunner;
import org.junit.runner.RunWith;

import static java.lang.String.join;
import static j8spec.J8Spec.*;
import static org.junit.Assert.*;

@DefinedOrder
@RunWith(J8SpecRunner.class)
public class L4ClientTest {
  static {
    if (L4Tests.runIntegrationTests) {
      it("Interacts with an Rqlite instance", () -> {
        L4Client rq = L4Tests.localClient();

        System.out.println(rq.status().toString());
        System.out.println(rq.nodes().toString());
        System.out.println(rq.ready());

        L4Response res0 = rq.executeSingle(join("\n", "",
          "CREATE TABLE IF NOT EXISTS users (",
          "  id INTEGER PRIMARY KEY AUTOINCREMENT,",
          "  name TEXT NOT NULL,",
          "  email TEXT NOT NULL UNIQUE,",
          "  age INTEGER",
          ")"
        ));
        assertEquals(200, res0.statusCode);
        res0.print(System.out);

        L4Response res1 = rq.querySingle("SELECT * FROM users");
        assertEquals(200, res1.statusCode);
        res1.print(System.out);

        if (res1.results != null) {
          java.util.List<io.rqlite.client.L4Result> rl = res1.results;
          java.util.List<java.util.List<String>> vals = rl.get(0).values;
          if (vals == null || vals.isEmpty()) {
            L4Response res2 = rq.execute(
              true,
              new L4Statement().sql("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)"),
              new L4Statement().sql("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)"),
              new L4Statement().sql("INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 35)")
            );
            System.out.println(res2);
          }
        }

        L4Response res3 = rq.querySingle("SELECT * FROM users WHERE age > ?", 30);
        assertEquals(200, res3.statusCode);
        res3.print(System.out);
      });
    }
  }
}
