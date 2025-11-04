package io.rqlite;

import io.rqlite.json.Json;
import j8spec.annotation.DefinedOrder;
import j8spec.junit.J8SpecRunner;
import org.junit.runner.RunWith;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;
import io.rqlite.json.JsonValue;

import static j8spec.J8Spec.*;

@DefinedOrder
@RunWith(J8SpecRunner.class)
public class JsonTest {
  static {
    it("Parses/Prints JSON", () -> {
      try (InputStream is = JsonTest.class.getResourceAsStream("/example.json")) {
        JsonValue jv = Json.parse(new InputStreamReader(Objects.requireNonNull(is)));
        System.out.println(jv.toString());
      }
    });
  }
}
