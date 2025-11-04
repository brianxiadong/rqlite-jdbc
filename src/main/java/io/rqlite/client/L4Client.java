package io.rqlite.client;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import io.rqlite.jdbc.L4Log;
import io.rqlite.json.Json;
import io.rqlite.json.JsonObject;
import io.rqlite.json.JsonValue;

import static io.rqlite.client.L4Response.*;
import static io.rqlite.client.L4Err.*;
import static java.lang.String.format;

public class L4Client implements Closeable {

  private final String baseUrl;
  private final String executeURL;
  private final String queryURL;
  private final String statusURL;
  private final String nodesURL;
  private final String readyURL;

  public String basicAuthUser = "";
  private String basicAuthPass = "";
  private List<L4Response> buffer;

  public L4Client(String baseURL) {
    this.baseUrl = Objects.requireNonNull(baseURL);
    this.executeURL = baseURL + "/db/execute";
    this.queryURL = baseURL + "/db/query";
    this.statusURL = baseURL + "/status";
    this.nodesURL = baseURL + "/nodes";
    this.readyURL = baseURL + "/readyz";
  }

  private static L4HttpResp sendRequest(String url, String method, String body) throws Exception {
    URL u = URI.create(url).toURL();
    HttpURLConnection conn = (HttpURLConnection) u.openConnection();
    conn.setRequestMethod(method);
    int timeoutMs = L4Options.timeoutSec > 0 ? (int) (L4Options.timeoutSec * 1000) : 0;
    if (timeoutMs > 0) {
      conn.setConnectTimeout(timeoutMs);
      conn.setReadTimeout(timeoutMs);
    }
    conn.setDoInput(true);
    if ("POST".equals(method)) {
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");
      if (body != null) {
        OutputStream os = conn.getOutputStream();
        os.write(body.getBytes("UTF-8"));
        os.flush();
        os.close();
      }
    }
    int status = conn.getResponseCode();
    InputStream is = status >= 400 ? conn.getErrorStream() : conn.getInputStream();
    String respBody = null;
    if (is != null) {
      BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
      br.close();
      respBody = sb.toString();
    }
    conn.disconnect();
    return new L4HttpResp(status, respBody);
  }

  private L4HttpResp doPostRequest(String url, String body) {
    int statusCode = -1;
    try {
      L4Log.trace("{} - POST {}", this, body);
      // Add auth via header when sending
      String authHeader = buildAuthHeader();
      if (authHeader != null) {
        // We set header inside sendRequest by passing body; but HttpURLConnection requires header before connect
        // Here we rely on sendRequest to set Content-Type; we cannot set Authorization there easily without conn reference
        // Workaround: open connection here to set header, then delegate write/read
        URL u = URI.create(url).toURL();
        HttpURLConnection conn = (HttpURLConnection) u.openConnection();
        conn.setRequestMethod("POST");
        int timeoutMs = L4Options.timeoutSec > 0 ? (int) (L4Options.timeoutSec * 1000) : 0;
        if (timeoutMs > 0) {
          conn.setConnectTimeout(timeoutMs);
          conn.setReadTimeout(timeoutMs);
        }
        conn.setDoInput(true);
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Authorization", authHeader);
        if (body != null) {
          OutputStream os = conn.getOutputStream();
          os.write(body.getBytes("UTF-8"));
          os.flush();
          os.close();
        }
        int status = conn.getResponseCode();
        InputStream is = status >= 400 ? conn.getErrorStream() : conn.getInputStream();
        String respBody = null;
        if (is != null) {
          BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
          StringBuilder sb = new StringBuilder();
          String line;
          while ((line = br.readLine()) != null) {
            sb.append(line);
          }
          br.close();
          respBody = sb.toString();
        }
        conn.disconnect();
        statusCode = status;
        return checkResponse(new L4HttpResp(status, respBody));
      } else {
        L4HttpResp res = sendRequest(url, "POST", body);
        statusCode = res.statusCode();
        return checkResponse(res);
      }
    } catch (Exception e) {
      throw new IllegalStateException(format("HTTP POST error: (%d) [%s]", statusCode, url), e);
    }
  }

  private L4HttpResp doJSONPostRequest(String url, String body) {
    return doPostRequest(url, body);
  }

  private L4HttpResp doGetRequest(String url) {
    int statusCode = -1;
    try {
      String authHeader = buildAuthHeader();
      URL u = URI.create(url).toURL();
      HttpURLConnection conn = (HttpURLConnection) u.openConnection();
      conn.setRequestMethod("GET");
      int timeoutMs = L4Options.timeoutSec > 0 ? (int) (L4Options.timeoutSec * 1000) : 0;
      if (timeoutMs > 0) {
        conn.setConnectTimeout(timeoutMs);
        conn.setReadTimeout(timeoutMs);
      }
      conn.setDoInput(true);
      if (authHeader != null) {
        conn.setRequestProperty("Authorization", authHeader);
      }
      int status = conn.getResponseCode();
      InputStream is = status >= 400 ? conn.getErrorStream() : conn.getInputStream();
      String respBody = null;
      if (is != null) {
        BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
          sb.append(line);
        }
        br.close();
        respBody = sb.toString();
      }
      conn.disconnect();
      statusCode = status;
      return checkResponse(new L4HttpResp(status, respBody));
    } catch (Exception e) {
      throw new IllegalStateException(format("HTTP GET error: (%d) [%s]", statusCode, url), e);
    }
  }

  private String buildAuthHeader() {
    if (!basicAuthUser.isEmpty() || !basicAuthPass.isEmpty()) {
      String auth = basicAuthUser + ":" + basicAuthPass;
      String encoded = Base64.getEncoder().encodeToString(auth.getBytes());
      return "Basic " + encoded;
    }
    return null;
  }

  public L4Client withBasicAuth(String username, String password) {
    this.basicAuthUser = username;
    this.basicAuthPass = password;
    return this;
  }

  public boolean isBuffering() {
    return this.buffer != null;
  }

  public void startBuffer() {
    if (buffer == null) {
      buffer = new ArrayList<L4Response>();
    }
  }

  private L4Response doExecute(boolean transaction, L4Statement ... statements) {
    String queryParams = L4Options.queryParams(transaction);
    String url = executeURL + queryParams;
    String body = L4Statement.toArray(statements).toString();
    L4HttpResp resp = doJSONPostRequest(url, body);
    String rb = resp.body();
    JsonObject node = Json.parse(rb).asObject();
    return response(resp.statusCode(), node);
  }

  public void stopBuffer(boolean commit, Consumer<L4Response> responseFn) {
    if (commit && buffer != null && !buffer.isEmpty()) {
      L4Statement[] statements = buffer.stream()
        .flatMap(res -> Arrays.stream(res.statements))
        .toArray(L4Statement[]::new);
      buffer.clear();
      responseFn.accept(doExecute(true, statements));
    }
    buffer = null;
  }

  public L4Response execute(boolean transaction, L4Statement ... statements) {
    if (isBuffering()) {
      L4Log.trace("{} - defer: {}", this, Arrays.toString(statements));
      L4Response res = deferred(statements);
      res.results = new ArrayList<L4Result>();
      res.results.add(new L4Result(new JsonObject()));
      this.buffer.add(res);
      return res;
    }
    return doExecute(transaction, statements);
  }

  public L4Response executeSingle(String statement, Object... args) {
    L4Response res = execute(true, new L4Statement().sql(statement).withPositionalParams(args));
    checkResult(res.first());
    return res;
  }

  public L4Response query(L4Statement ... statements) {
    String body = L4Statement.toArray(statements).toString();
    String queryParams = L4Options.queryParams(false);
    L4HttpResp resp = doJSONPostRequest(queryURL + queryParams, body);
    String rb = resp.body();
    JsonObject node = Json.parse(rb).asObject();
    return response(resp.statusCode(), node);
  }

  public L4Response querySingle(String statement, Object... args) {
    L4Response res = query(new L4Statement().sql(statement).withPositionalParams(args));
    checkResult(res.first());
    return res;
  }

  public JsonValue status() {
    L4HttpResp resp = doGetRequest(statusURL);
    return Json.parse(resp.body());
  }

  public JsonValue nodes() {
    L4HttpResp resp = doGetRequest(nodesURL);
    return Json.parse(resp.body());
  }

  public String ready() {
    L4HttpResp resp = doGetRequest(readyURL);
    return resp.body();
  }

  public void withTxTimeoutSec(long txTimeoutSec) {
    if (txTimeoutSec < 0) {
      throw new IllegalArgumentException(format("Invalid timeout [%d]", txTimeoutSec));
    }
    L4Options.timeoutSec = txTimeoutSec == 0 ? -1 : txTimeoutSec;
  }

  public long getTxTimeoutSec() {
    return L4Options.timeoutSec;
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  @Override public void close() {
    // nothing to close for HttpURLConnection
  }

  @Override public String toString() {
    return String.format("l4c [%08x, %03d]", this.hashCode(), buffer == null ? -1 : buffer.size());
  }
}
