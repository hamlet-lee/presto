/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.spi.type.Varchars;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.mysql.jdbc.Driver;
import com.mysql.jdbc.Statement;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import static java.util.Locale.ENGLISH;

public class MySqlClient
        extends BaseJdbcClient
{
    private Set<String> schemaRealNames = new HashSet<>();
    private Set<String> tableRealNames = new HashSet<>();

    @Inject
    public MySqlClient(JdbcConnectorId connectorId, BaseJdbcConfig config, MySqlConfig mySqlConfig)
            throws SQLException
    {
        super(connectorId, config, "`", new Driver());
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        if (mySqlConfig.isAutoReconnect()) {
            connectionProperties.setProperty("autoReconnect", String.valueOf(mySqlConfig.isAutoReconnect()));
            connectionProperties.setProperty("maxReconnects", String.valueOf(mySqlConfig.getMaxReconnects()));
        }
        if (mySqlConfig.getConnectionTimeout() != null) {
            connectionProperties.setProperty("connectTimeout", String.valueOf(mySqlConfig.getConnectionTimeout().toMillis()));
        }
//        this.schemaRealNames = mySqlConfig.getSchemaRealNames();
    }

    @Override
    public Set<String> getSchemaNames()
    {
        // for MySQL, we need to list catalogs instead of schemas
        try (Connection connection = driver.connect(connectionUrl, connectionProperties);
                ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String realName = resultSet.getString("TABLE_CAT");
                String schemaName = resultSet.getString("TABLE_CAT").toLowerCase(ENGLISH);
                //if have case difference, we can remember the difference
                if (!realName.equals(schemaName)) {
                    if (!schemaRealNames.contains(realName)) {
                        schemaRealNames.add(realName);
                    }
                }
                // skip internal schemas
                if (!schemaName.equals("information_schema") && !schemaName.equals("mysql")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        for (String realName : schemaRealNames) {
            String unrealName = realName.toLowerCase(ENGLISH);
            int pos = sql.indexOf("`" + unrealName + "`");
            if (pos >= 0) {
                sql = sql.replaceAll("`" + unrealName + "`", "`" + realName + "`");
            }
        }
        PreparedStatement statement = connection.prepareStatement(sql);
        if (statement.isWrapperFor(Statement.class)) {
            statement.unwrap(Statement.class).enableStreamingResults();
        }
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        // MySQL maps their "database" to SQL catalogs and does not have schemas
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        String realSchemaName = fixSchemaName(schemaName);
        String realTableName = fixTableName(tableName);
        return metadata.getTables(
                realSchemaName,
                null,
                escapeNamePattern(tableName, escape),
                new String[] {"TABLE", "VIEW"});
    }

    private String fixTableName(String tableName)
    {
        if (tableName == null) {
            return null;
        }
        for (String realTableName : tableRealNames) {
            if (realTableName.toLowerCase(ENGLISH).equals(tableName)) {
                return realTableName;
            }
        }
        return tableName;
    }

    private String fixSchemaName(String schemaName)
    {
        for (String realSchemaName : schemaRealNames) {
            if (realSchemaName.toLowerCase(ENGLISH).equals(schemaName)) {
                return realSchemaName;
            }
        }
        return schemaName;
    }

    @Override
    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException
    {
        // MySQL uses catalogs instead of schemas
        String realSchemaName = resultSet.getString("TABLE_CAT");
        String realTableName = resultSet.getString("TABLE_NAME");
        String schemaName = realSchemaName.toLowerCase(ENGLISH);
        String tableName = realTableName.toLowerCase(ENGLISH);
        if (!tableName.equals(realTableName)) {
            tableRealNames.add(realTableName);
        }
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    protected String toSqlType(Type type)
    {
        if (Varchars.isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.getLength() <= 255) {
                return "tinytext";
            }
            if (varcharType.getLength() <= 65535) {
                return "text";
            }
            if (varcharType.getLength() <= 16777215) {
                return "mediumtext";
            }
            return "longtext";
        }

        String sqlType = super.toSqlType(type);
        switch (sqlType) {
            case "varbinary":
                return "mediumblob";
            case "time with timezone":
                return "time";
            case "timestamp":
            case "timestamp with timezone":
                return "datetime";
        }
        return sqlType;
    }
}
