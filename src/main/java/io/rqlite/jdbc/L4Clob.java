package io.rqlite.jdbc;

import javax.sql.rowset.serial.SerialClob;
import java.io.*;
import java.sql.Clob;
import java.sql.SQLException;

public class L4Clob implements Clob {

  private StringBuilder data = new StringBuilder();

  @Override public long length() throws SQLException {
    return data.length();
  }

  @Override public String getSubString(long pos, int length) throws SQLException {
    int offset = (int) (pos - 1);
    return data.substring(offset, offset + length);
  }

  @Override public Reader getCharacterStream() throws SQLException {
    return new StringReader(data.toString());
  }

  @Override public InputStream getAsciiStream() throws SQLException {
    try {
      return new ByteArrayInputStream(data.toString().getBytes("US-ASCII"));
    } catch (java.io.UnsupportedEncodingException e) {
      throw new SQLException("Unsupported ASCII encoding", e);
    }
  }

  @Override public int setString(long pos, String str) throws SQLException {
    int offset = (int) (pos - 1);
    String substr = str.substring(offset, Math.min(str.length(), offset + str.length()));
    data.replace(offset, offset + substr.length(), substr);
    return substr.length();
  }

  @Override public int setString(long pos, String str, int offset, int len) throws SQLException {
    int base = (int) (pos - 1);
    String substr = str.substring(offset, offset + len);
    data.replace(base, base + substr.length(), substr);
    return substr.length();
  }

  @Override public OutputStream setAsciiStream(long pos) throws SQLException {
    throw new SQLException("Feature not supported");
  }

  @Override public Writer setCharacterStream(long pos) throws SQLException {
    throw new SQLException("Feature not supported");
  }

  @Override public Reader getCharacterStream(long pos, long length) throws SQLException {
    return new StringReader(getSubString(pos, (int) length));
  }

  @Override public long position(String searchstr, long start) throws SQLException {
    int idx = data.indexOf(searchstr, (int) (start - 1));
    return idx >= 0 ? idx + 1 : -1;
  }

  @Override public long position(Clob searchstr, long start) throws SQLException {
    return position(searchstr.getSubString(1, (int) searchstr.length()), start);
  }

  @Override public void truncate(long len) throws SQLException {
    if (len < data.length()) {
      data.setLength((int) len);
    }
  }

  @Override public void free() throws SQLException {
    data.setLength(0);
  }

}
