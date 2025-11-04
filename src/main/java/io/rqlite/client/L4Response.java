package io.rqlite.client;

import io.rqlite.json.JsonObject;
import io.rqlite.json.JsonArray;
import io.rqlite.json.JsonValue;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class L4Response {

  public L4Statement[]  statements;

  public List<L4Result> results;
  public Float          time;
  public int            statusCode;

  public void print(PrintStream out) {
    for (L4Result res : results) {
      res.print(out);
    }
  }

  public L4Result first() {
    if (results != null && !results.isEmpty()) {
      return results.get(0);
    }
    return null;
  }

  public static L4Response response(int statusCode, JsonObject obj) {
    L4Response r = new L4Response();
    r.statusCode = statusCode;
    JsonValue res = obj.get("results");
    if (res != null) {
      JsonArray resultsArray = res.asArray();
      r.results = new ArrayList<L4Result>();
      for (JsonValue resultValue : resultsArray) {
        r.results.add(new L4Result(resultValue.asObject()));
      }
    } else {
      r.results = new ArrayList<L4Result>();
    }
    r.time = obj.get("time") != null ? obj.getFloat("time", -1) : null;
    return r;
  }

  public static L4Response deferred(L4Statement[] statements) {
    L4Response r = new L4Response();
    r.statements = Objects.requireNonNull(statements);
    return r;
  }

  @Override public String toString() {
    return String.format(
      "[st: %s, res: %s, time: %.4f, status: %d]",
      statements == null ? "[]" : Arrays.toString(statements),
      results, time, statusCode
    );
  }

}