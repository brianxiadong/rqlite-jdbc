package io.rqlite.client;

public class L4HttpResp {
  private final int statusCode;
  private final String body;

  public L4HttpResp(int statusCode, String body) {
    this.statusCode = statusCode;
    this.body = body;
  }

  public int statusCode() {
    return statusCode;
  }

  public String body() {
    return body;
  }

  @Override public String toString() {
    return "L4HttpResp{" +
      "statusCode=" + statusCode +
      ", body=" + (body == null ? "null" : body) +
      '}';
  }
}