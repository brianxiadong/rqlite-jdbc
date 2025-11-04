package io.rqlite.jdbc;

import io.rqlite.client.L4Client;
import io.rqlite.client.L4Http;
import io.rqlite.client.L4Options;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

import static io.rqlite.jdbc.L4Err.*;
import static io.rqlite.jdbc.L4Jdbc.*;
import static io.rqlite.client.L4Options.*;
import static java.lang.String.format;

public class L4Driver implements Driver {

  private static final String JDBC_URL_PREFIX = "jdbc:rqlite:";
  private static final Logger log = Logger.getLogger(L4Driver.class.getName());

  static {
    try {
      DriverManager.registerDriver(new L4Driver());
    } catch (SQLException e) {
      throw new RuntimeException("Failed to register L4Driver", e);
    }
  }

  @Override public boolean acceptsURL(String url) {
    if (url == null) {
      return false;
    }
    return url.startsWith(JDBC_URL_PREFIX);
  }

  public Map<String, String> getQueryParams(String url) throws SQLException {
    if (!acceptsURL(url)) {
      throw badParam(format("Invalid rqlite JDBC URL: %s", url));
    }
    try {
      String rqliteUrl = url.substring(JDBC_URL_PREFIX.length());
      String[] urlParts = rqliteUrl.split("\\?", 2);
      Map<String, String> queryParams = new HashMap<String, String>();
      queryParams.put(kBaseUrl, urlParts[0]);
      if (urlParts.length > 1) {
        String[] params = urlParts[1].split("&");
        for (String param : params) {
          String[] keyValue = param.split("=", 2);
          if (keyValue.length == 2) {
            queryParams.put(keyValue[0].toLowerCase(), keyValue[1]);
          }
        }
      }
      return queryParams;
    } catch (Exception e) {
      throw badParam(e);
    }
  }

  private void configureTLSIfNeeded() throws SQLException {
    try {
      boolean isHttps = L4Options.baseUrl.toLowerCase().startsWith("https://");
      String cacert = L4Options.cacert;
      if (isHttps) {
        if (L4Options.insecure) {
          L4Http.configureInsecureTLS();
        } else if (cacert != null && !cacert.isEmpty()) {
          L4Http.configureTLSWithCACert(cacert);
        }
      }
    } catch (Exception e) {
      throw badParam(e);
    }
  }

  public L4Client createL4Client() throws SQLException {
    try {
      String user = L4Options.user;
      String password = L4Options.password;
      L4Client client = new L4Client(L4Options.baseUrl);
      if (user != null && password != null) {
        return client.withBasicAuth(user, password);
      }
      return client;
    } catch (Exception e) {
      throw new SQLException("Failed to create L4Client: " + e.getMessage(), e);
    }
  }

  private Properties mergeProperties(Properties info, Map<String, String> queryParams) {
    Properties merged = new Properties();
    for (Map.Entry<String, String> entry : queryParams.entrySet()) {
      merged.setProperty(entry.getKey(), entry.getValue());
    }
    if (info != null) {
      merged.putAll(info);
    }
    return merged;
  }

  @Override public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    try {
      L4Options.update(mergeProperties(info, getQueryParams(url)));
      configureTLSIfNeeded();
      L4Client client = createL4Client();
      return new L4Conn(client);
    } catch (Exception e) {
      throw badState("Failed to establish connection", e);
    }
  }

  @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
    Properties mergedProps = mergeProperties(info, new HashMap<String, String>());
    DriverPropertyInfo[] props = new DriverPropertyInfo[10];

    props[0] = new DriverPropertyInfo(kUser, mergedProps.getProperty(kUser));
    props[0].description = "Username for rqlite authentication";
    props[0].required = false;

    props[1] = new DriverPropertyInfo(kPassword, mergedProps.getProperty(kPassword));
    props[1].description = "Password for rqlite authentication";
    props[1].required = false;

    props[2] = new DriverPropertyInfo(kTimeoutSec, mergedProps.getProperty(kTimeoutSec, String.valueOf(L4Options.timeoutSec)));
    props[2].description = "Timeout in seconds";
    props[2].required = false;

    props[3] = new DriverPropertyInfo(kQueue, mergedProps.getProperty(kQueue, String.valueOf(L4Options.queue)));
    props[3].description = "Enable queue mode";
    props[3].required = false;

    props[4] = new DriverPropertyInfo(kWait, mergedProps.getProperty(kWait, String.valueOf(L4Options.wait)));
    props[4].description = "Enable wait mode";
    props[4].required = false;

    props[5] = new DriverPropertyInfo(kLevel, mergedProps.getProperty(kLevel, L4Options.level.toString()));
    props[5].description = "Consistency level (none, weak, linearizable)";
    props[5].required = false;

    props[6] = new DriverPropertyInfo(kLinearizableTimeoutSec, mergedProps.getProperty(kLinearizableTimeoutSec, String.valueOf(L4Options.linearizableTimeoutSec)));
    props[6].description = "Linearizable timeout in seconds";
    props[6].required = false;

    props[7] = new DriverPropertyInfo(kFreshnessSec, mergedProps.getProperty(kFreshnessSec, String.valueOf(L4Options.freshnessSec)));
    props[7].description = "Freshness in seconds";
    props[7].required = false;

    props[8] = new DriverPropertyInfo(kFreshnessStrict, mergedProps.getProperty(kFreshnessStrict, String.valueOf(L4Options.freshnessStrict)));
    props[8].description = "Enable strict freshness";
    props[8].required = false;

    props[9] = new DriverPropertyInfo(kCaCert, mergedProps.getProperty(kCaCert));
    props[9].description = "Path to CA certificate for HTTPS connections";
    props[9].required = false;

    return props;
  }

  @Override public int getMajorVersion() {
    return driverVersionMajor();
  }

  @Override public int getMinorVersion() {
    return driverVersionMinor();
  }

  @Override public boolean jdbcCompliant() {
    return false;
  }

  @Override public Logger getParentLogger() {
    return log;
  }

}