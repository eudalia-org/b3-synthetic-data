package com.eudalia.datagen;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.Properties;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Distributed JDBC writer for INSERT statements containing Oracle expressions. */
public final class OracleAuditJdbcWriter {
    private OracleAuditJdbcWriter() {}

    public static void save(
            Dataset<Row> data,
            String sql,
            String url,
            String user,
            String password,
            String driverClass,
            int readTimeoutMs,
            int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        data.foreachPartition(new PartitionWriter(
                data.schema(), sql, url, user, password, driverClass,
                readTimeoutMs, batchSize));
    }

    private static final class PartitionWriter
            implements ForeachPartitionFunction<Row>, Serializable {
        private static final long serialVersionUID = 1L;

        private final StructType schema;
        private final String sql;
        private final String url;
        private final String user;
        private final String password;
        private final String driverClass;
        private final int readTimeoutMs;
        private final int batchSize;

        private PartitionWriter(
                StructType schema,
                String sql,
                String url,
                String user,
                String password,
                String driverClass,
                int readTimeoutMs,
                int batchSize) {
            this.schema = schema;
            this.sql = sql;
            this.url = url;
            this.user = user;
            this.password = password;
            this.driverClass = driverClass;
            this.readTimeoutMs = readTimeoutMs;
            this.batchSize = batchSize;
        }

        @Override
        public void call(Iterator<Row> rows) throws Exception {
            if (!rows.hasNext()) {
                return;
            }

            Properties properties = new Properties();
            properties.setProperty("user", user);
            properties.setProperty("password", password);
            properties.setProperty("oracle.jdbc.ReadTimeout", Integer.toString(readTimeoutMs));

            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            Driver driver = (Driver) Class.forName(driverClass, true, loader)
                    .getDeclaredConstructor().newInstance();
            Connection connection = driver.connect(url, properties);
            if (connection == null) {
                throw new SQLException(driverClass + " did not accept JDBC URL");
            }

            PreparedStatement statement = null;
            Exception failure = null;
            boolean committed = false;
            try {
                connection.setAutoCommit(false);
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                statement = connection.prepareStatement(sql);
                int pending = 0;
                do {
                    bind(statement, rows.next(), schema);
                    statement.addBatch();
                    pending++;
                    if (pending == batchSize) {
                        statement.executeBatch();
                        statement.clearBatch();
                        pending = 0;
                    }
                } while (rows.hasNext());
                if (pending > 0) {
                    statement.executeBatch();
                }
                connection.commit();
                committed = true;
            } catch (Exception error) {
                failure = error;
                try {
                    connection.rollback();
                } catch (SQLException rollbackError) {
                    failure.addSuppressed(rollbackError);
                }
            } finally {
                failure = close(statement, committed, failure);
                failure = close(connection, committed, failure);
            }
            if (failure != null) {
                throw failure;
            }
        }

        private static Exception close(
                AutoCloseable resource, boolean committed, Exception failure) {
            if (resource == null) {
                return failure;
            }
            try {
                resource.close();
            } catch (Exception closeError) {
                if (failure != null) {
                    failure.addSuppressed(closeError);
                } else if (!committed) {
                    return closeError;
                }
            }
            return failure;
        }
    }

    private static void bind(PreparedStatement statement, Row row, StructType schema)
            throws SQLException {
        StructField[] fields = schema.fields();
        for (int i = 0; i < fields.length; i++) {
            int parameter = i + 1;
            DataType type = fields[i].dataType();
            if (row.isNullAt(i)) {
                statement.setNull(parameter, jdbcType(type));
            } else if (type.equals(DataTypes.StringType)) {
                statement.setString(parameter, row.getString(i));
            } else if (type.equals(DataTypes.BinaryType)) {
                statement.setBytes(parameter, (byte[]) row.get(i));
            } else if (type.equals(DataTypes.BooleanType)) {
                statement.setBoolean(parameter, row.getBoolean(i));
            } else if (type.equals(DataTypes.ByteType)) {
                statement.setByte(parameter, row.getByte(i));
            } else if (type.equals(DataTypes.ShortType)) {
                statement.setShort(parameter, row.getShort(i));
            } else if (type.equals(DataTypes.IntegerType)) {
                statement.setInt(parameter, row.getInt(i));
            } else if (type.equals(DataTypes.LongType)) {
                statement.setLong(parameter, row.getLong(i));
            } else if (type.equals(DataTypes.FloatType)) {
                statement.setFloat(parameter, row.getFloat(i));
            } else if (type.equals(DataTypes.DoubleType)) {
                statement.setDouble(parameter, row.getDouble(i));
            } else if (type instanceof DecimalType) {
                statement.setBigDecimal(parameter, row.getDecimal(i));
            } else if (type.equals(DataTypes.DateType)) {
                statement.setDate(parameter, row.getDate(i));
            } else if (type.equals(DataTypes.TimestampType)) {
                statement.setTimestamp(parameter, row.getTimestamp(i));
            } else if (type.equals(DataTypes.TimestampNTZType)) {
                statement.setTimestamp(parameter,
                        java.sql.Timestamp.valueOf((java.time.LocalDateTime) row.get(i)));
            } else {
                throw new SQLException("Unsupported Spark JDBC type: " + type.typeName());
            }
        }
    }

    private static int jdbcType(DataType type) {
        if (type.equals(DataTypes.StringType)) return Types.VARCHAR;
        if (type.equals(DataTypes.BinaryType)) return Types.VARBINARY;
        if (type.equals(DataTypes.BooleanType)) return Types.BOOLEAN;
        if (type.equals(DataTypes.ByteType)) return Types.TINYINT;
        if (type.equals(DataTypes.ShortType)) return Types.SMALLINT;
        if (type.equals(DataTypes.IntegerType)) return Types.INTEGER;
        if (type.equals(DataTypes.LongType)) return Types.BIGINT;
        if (type.equals(DataTypes.FloatType)) return Types.REAL;
        if (type.equals(DataTypes.DoubleType)) return Types.DOUBLE;
        if (type instanceof DecimalType) return Types.DECIMAL;
        if (type.equals(DataTypes.DateType)) return Types.DATE;
        if (type.equals(DataTypes.TimestampType)) return Types.TIMESTAMP;
        if (type.equals(DataTypes.TimestampNTZType)) return Types.TIMESTAMP;
        return Types.OTHER;
    }
}
