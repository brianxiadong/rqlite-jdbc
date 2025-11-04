package io.rqlite.jdbc;

import io.rqlite.client.L4Client;
import io.rqlite.client.L4Response;
import io.rqlite.client.L4Statement;
import io.rqlite.client.L4Result;

import java.sql.*;
import java.util.*;

import static io.rqlite.jdbc.L4Jdbc.*;
import static io.rqlite.jdbc.L4Err.*;
import static io.rqlite.client.L4Err.*;

public class L4St implements Statement {

  protected final L4Conn            conn;
  protected final L4Client          client;
  protected final List<L4Statement> batch = new ArrayList<>();

  protected boolean     isClosed = false;
  protected L4Rs        currentResultSet = null;
  protected L4Response  currentResponse = null;
  protected int         maxRows = -1;
  protected int         fetchSize = 0;
  protected boolean     closeOnCompletion = false;
  protected int         currentResultIndex = -1;

  public L4St(L4Client client, L4Conn conn) {
    this.client = Objects.requireNonNull(client);
    this.conn = conn;
  }

  public L4St(L4Client client) {
    this(client, null);
  }

  protected boolean isAutoCommit() throws SQLException {
    return conn != null && conn.getAutoCommit();
  }

  protected void checkClosed() throws SQLException {
    if (isClosed) {
      throw stClosed(false);
    }
  }

  protected void closeCurrentResultSet() throws SQLException {
    if (currentResultSet != null && !currentResultSet.isClosed()) {
      currentResultSet.close();
    }
    currentResultSet = null;
  }

  private L4Response runRaw(String sql) throws SQLException {
    boolean sel = isSelect(sql);
    L4Statement[] sta = split(sql);
    L4Response res = sel ? client.query(sta) : client.execute(isAutoCommit(), sta);
    for (L4Result result : res.results) {
      checkResult(result);
    }
    return res;
  }

  @Override public ResultSet executeQuery(String sql) throws SQLException {
    checkClosed();
    closeCurrentResultSet();
    currentResultIndex = -1;
    if (sql == null || sql.trim().isEmpty()) {
      throw badStatement();
    }
    try {
      currentResponse = runRaw(sql);
      currentResultIndex = 0;
      currentResultSet = new L4Rs(currentResponse.first(), this).clampTo(maxRows);
      return currentResultSet;
    } catch (Exception e) {
      throw badQuery(e);
    }
  }

  @Override public int executeUpdate(String sql) throws SQLException {
    checkClosed();
    closeCurrentResultSet();
    currentResultIndex = -1;
    if (sql == null || sql.trim().isEmpty()) {
      throw badStatement();
    }
    try {
      currentResponse = client.execute(isAutoCommit(), new L4Statement().sql(sql));
      L4Result result = checkResult(currentResponse.first());
      return result.rowsAffected != null ? result.rowsAffected : 0;
    } catch (Exception e) {
      throw badUpdate(e);
    }
  }

  @Override public void close() throws SQLException {
    if (!isClosed) {
      closeCurrentResultSet();
      batch.clear();
      currentResultIndex = -1;
      currentResponse = null;
      closeOnCompletion = false;
      isClosed = true;
    }
  }

  @Override public int getMaxFieldSize() throws SQLException {
    checkClosed();
    throw notSupported("Maximum field size");
  }

  @Override public void setMaxFieldSize(int max) throws SQLException {
    checkClosed();
    throw notSupported("Maximum field size");
  }

  @Override public int getMaxRows() throws SQLException {
    checkClosed();
    return maxRows;
  }

  @Override public void setMaxRows(int max) throws SQLException {
    checkClosed();
    if (max < 0) {
      throw badMaxRows();
    }
    this.maxRows = max;
  }

  @Override public void setEscapeProcessing(boolean enable) throws SQLException {
    checkClosed();
  }

  @Override public int getQueryTimeout() throws SQLException {
    checkClosed();
    return (int) (client.getTxTimeoutSec() == -1 ? 0 : client.getTxTimeoutSec());
  }

  @Override public void setQueryTimeout(int seconds) throws SQLException {
    checkClosed();
    try {
      client.withTxTimeoutSec(seconds);
    } catch (Exception e) {
      throw badParam(e);
    }
  }

  @Override public void cancel() throws SQLException {
    checkClosed();
    throw notSupported("Statement cancellation");
  }

  @Override public SQLWarning getWarnings() throws SQLException {
    checkClosed();
    if (currentResponse != null) {
      SQLWarning root = (SQLWarning) null;
      for (L4Result res : currentResponse.results) {
        if (res.error != null) {
          SQLWarning warn = warnQuery(res.error);
          if (root == null) {
            root = warn;
          } else {
            root.setNextWarning(warn);
          }
        }
      }
      return root;
    }
    return null;
  }

  @Override public void clearWarnings() throws SQLException {
    checkClosed();
    if (currentResponse != null) {
      for (L4Result res : currentResponse.results) {
        res.error = null;
      }
    }
  }

  @Override public void setCursorName(String name) throws SQLException {
    checkClosed();
    throw notSupported("Positioned updates via cursor name");
  }

  @Override public boolean execute(String sql) throws SQLException {
    checkClosed();
    closeCurrentResultSet();
    currentResultIndex = -1;
    if (sql == null || sql.trim().isEmpty()) {
      throw badStatement();
    }
    try {
      currentResponse = runRaw(sql);
      if (currentResponse.results == null || currentResponse.results.isEmpty()) {
        return false;
      }
      currentResultIndex = 0;
      L4Result first = currentResponse.results.get(0);
      if (first.columns == null || first.columns.isEmpty()) {
        currentResultSet = null;
        return false;
      }
      currentResultSet = new L4Rs(first, this).clampTo(maxRows);
      return true;
    } catch (Exception e) {
      throw badExec(e);
    }
  }

  @Override public ResultSet getResultSet() throws SQLException {
    checkClosed();
    return currentResultSet;
  }

  @Override public int getUpdateCount() throws SQLException {
    checkClosed();
    if (currentResponse == null || currentResultIndex < 0 || currentResultIndex >= currentResponse.results.size()) {
      return -1;
    }
    L4Result result = currentResponse.results.get(currentResultIndex);
    // For SELECT results, or when rowsAffected is not provided, return -1 per JDBC spec.
    if ((result.columns != null && !result.columns.isEmpty()) || result.rowsAffected == null) {
      return -1;
    }
    return result.rowsAffected;
  }

  @Override public boolean getMoreResults() throws SQLException {
    checkClosed();
    if (currentResponse == null) {
      return false;
    }
    currentResultIndex++;
    if (currentResultIndex >= currentResponse.results.size()) {
      currentResultSet = null;
      return false;
    }
    L4Result result = currentResponse.results.get(currentResultIndex);
    if (result.columns == null || result.columns.isEmpty()) {
      currentResultSet = null;
      return false;
    }
    currentResultSet = new L4Rs(result, this).clampTo(maxRows);
    return true;
  }

  @Override public void setFetchDirection(int direction) throws SQLException {
    checkClosed();
    if (direction != ResultSet.FETCH_FORWARD) {
      throw notSupported("Only FETCH_FORWARD supported");
    }
  }

  @Override public int getFetchDirection() throws SQLException {
    checkClosed();
    // Statement-level fetch direction is not supported.
    throw notSupported("Fetch direction");
  }

  @Override public void setFetchSize(int rows) throws SQLException {
    checkClosed();
    if (rows < 0) {
      throw badFetchSize(rows);
    }
    this.fetchSize = rows;
  }

  @Override public int getFetchSize() throws SQLException {
    checkClosed();
    return fetchSize;
  }

  @Override public int getResultSetConcurrency() throws SQLException {
    checkClosed();
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override public int getResultSetType() throws SQLException {
    checkClosed();
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override public void addBatch(String sql) throws SQLException {
    checkClosed();
    if (sql == null || sql.trim().isEmpty()) {
      throw badStatement();
    }
    batch.add(new L4Statement().sql(sql));
  }

  @Override public void clearBatch() throws SQLException {
    checkClosed();
    batch.clear();
  }

  @Override public int[] executeBatch() throws SQLException {
    checkClosed();
    closeCurrentResultSet();
    if (batch.isEmpty()) {
      return new int[0];
    }
    try {
      currentResponse = client.execute(isAutoCommit(), batch.toArray(new L4Statement[0]));
      int[] updateCounts = new int[currentResponse.results.size()];
      for (int i = 0; i < currentResponse.results.size(); i++) {
        L4Result result = currentResponse.results.get(i);
        if (result.error != null) {
          throw new BatchUpdateException(result.error, SqlStateGeneralError, updateCounts, null);
        }
        updateCounts[i] = result.rowsAffected;
      }
      batch.clear();
      return updateCounts;
    } catch (Exception e) {
      throw badBatch(e);
    }
  }

  @Override public Connection getConnection() {
    return null;
  }

  @Override public boolean getMoreResults(int current) throws SQLException {
    checkClosed();
    if (current != CLOSE_CURRENT_RESULT) {
      throw notSupported("Result handling modes other than CLOSE_CURRENT_RESULT");
    }
    closeCurrentResultSet();
    return getMoreResults();
  }

  @Override public ResultSet getGeneratedKeys() throws SQLException {
    checkClosed();
    throw notSupported("Generated keys");
  }

  @Override public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    checkClosed();
    throw notSupported("Update with auto-generated keys");
  }

  @Override public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    checkClosed();
    throw notSupported("Update with column indexes");
  }

  @Override public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    checkClosed();
    throw notSupported("Update with column names");
  }

  @Override public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    checkClosed();
    throw notSupported("Execution with auto-generated keys");
  }

  @Override public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    checkClosed();
    throw notSupported("Execution with column indexes");
  }

  @Override public boolean execute(String sql, String[] columnNames) throws SQLException {
    checkClosed();
    throw notSupported("Execution with column names");
  }

  @Override public int getResultSetHoldability() throws SQLException {
    checkClosed();
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override public boolean isClosed() {
    return isClosed;
  }

  @Override public void setPoolable(boolean poolable) throws SQLException {
    checkClosed();
    throw notSupported("Statement pooling");
  }

  @Override public boolean isPoolable() throws SQLException {
    checkClosed();
    throw notSupported("Statement pooling");
  }

  @Override public void closeOnCompletion() throws SQLException {
    checkClosed();
    closeOnCompletion = true;
  }

  @Override public boolean isCloseOnCompletion() throws SQLException {
    checkClosed();
    return closeOnCompletion;
  }

  @Override public <T> T unwrap(Class<T> iface) throws SQLException {
    checkClosed();
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw notSupported("Unwrap to: " + iface.getName());
  }

  @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
    checkClosed();
    return iface.isAssignableFrom(getClass());
  }
}
