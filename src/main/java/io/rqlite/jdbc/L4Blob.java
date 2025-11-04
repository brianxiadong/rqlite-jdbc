package io.rqlite.jdbc;

import java.io.ByteArrayOutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;

public class L4Blob implements Blob {

    private ByteArrayOutputStream data = new ByteArrayOutputStream();

    @Override
    public long length() throws SQLException {
        return data.size();
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        byte[] bytes = data.toByteArray();
        int offset = (int) (pos - 1);
        return Arrays.copyOfRange(bytes, offset, offset + length);
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        int offset = (int) (pos - 1);
        byte[] current = data.toByteArray();
        ByteArrayOutputStream newData = new ByteArrayOutputStream();
        newData.write(current, 0, Math.min(offset, current.length));
        newData.write(bytes, 0, bytes.length);
        if (offset + bytes.length < current.length) {
            newData.write(current, offset + bytes.length, current.length - (offset + bytes.length));
        }
        data = newData;
        return bytes.length;
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        int base = (int) (pos - 1);
        byte[] subBytes = Arrays.copyOfRange(bytes, offset, offset + len);
        return setBytes(pos, subBytes);
    }

    @Override
    public java.io.InputStream getBinaryStream() throws SQLException {
        return new java.io.ByteArrayInputStream(data.toByteArray());
    }

    @Override
    public java.io.InputStream getBinaryStream(long pos, long length) throws SQLException {
        byte[] bytes = data.toByteArray();
        int offset = (int) (pos - 1);
        int end = Math.min(bytes.length, offset + (int) length);
        if (offset < 0 || offset > bytes.length) {
            throw new SQLException("Invalid BLOB position");
        }
        return new java.io.ByteArrayInputStream(Arrays.copyOfRange(bytes, offset, end));
    }

    @Override
    public java.io.OutputStream setBinaryStream(long pos) throws SQLException {
        throw new SQLException("Feature not supported");
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        byte[] bytes = data.toByteArray();
        int offset = (int) (start - 1);
        outer: for (int i = offset; i <= bytes.length - pattern.length; i++) {
            for (int j = 0; j < pattern.length; j++) {
                if (bytes[i + j] != pattern[j]) {
                    continue outer;
                }
            }
            return i + 1; // 1-based position
        }
        return -1;
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        if (pattern == null) {
            return -1;
        }
        byte[] pat = pattern.getBytes(1, (int) pattern.length());
        return position(pat, start);
    }

    @Override
    public void truncate(long len) throws SQLException {
        byte[] bytes = data.toByteArray();
        if (len < bytes.length) {
            data.reset();
            data.write(bytes, 0, (int) len);
        }
    }

    @Override
    public void free() throws SQLException {
        data.reset();
    }

}
