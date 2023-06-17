package schema;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

public class DataXGenerator {

    public static void main(String[] args) throws ClassNotFoundException {
        String driver = "oracle.jdbc.OracleDriver";
        Class.forName(driver);

        String oracleUrl = "jdbc:oracle:thin:@10.202.16.14:1521:ztodbkf";
        String oracleUsername = "ztotest";
        String oraclePassword = "Dyzto.#09";
        String tableName = "HS_EWBS_ARRIVALANDMOVE";

        try (Connection oracleConnection = DriverManager.getConnection(oracleUrl, oracleUsername, oraclePassword)) {
            DatabaseMetaData metadata = oracleConnection.getMetaData();
            ResultSet resultSet = metadata.getColumns(null, null, tableName, null);

            JsonObject dataxConfig = generateDataXConfig(resultSet);
            writeDataXConfigToFile(dataxConfig, "C:\\bigdata\\ideaWorkSpace\\test\\src\\main\\resources\\datax_config.json");

            generateMySQLTableDDL(resultSet, "C:\\bigdata\\ideaWorkSpace\\test\\src\\main\\resources\\mysql_table.sql");

            System.out.println("DataX JSON配置文件和MySQL建表语句生成成功！");
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private static JsonObject generateDataXConfig(ResultSet resultSet) throws SQLException {
        JsonObject dataxConfig = new JsonObject();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        JsonObject jobObject = new JsonObject();
        dataxConfig.add("job", jobObject);

        JsonArray contentArray = new JsonArray();
        jobObject.add("content", contentArray);

        contentArray.add(generateOracleReaderObject(resultSet));
        contentArray.add(generateMySQLWriterObject(resultSet));

        return dataxConfig;
    }

    private static JsonObject generateOracleReaderObject(ResultSet resultSet) throws SQLException {
        JsonObject oracleReaderObject = new JsonObject();
        oracleReaderObject.addProperty("name", "oraclereader");
        oracleReaderObject.add("parameter", generateOracleReaderParameter(resultSet));
        return oracleReaderObject;
    }

    private static JsonObject generateOracleReaderParameter(ResultSet resultSet) throws SQLException {
        JsonObject parameterObject = new JsonObject();

        JsonArray columnArray = new JsonArray();
        while (resultSet.next()) {
            String columnName = resultSet.getString("COLUMN_NAME");
            columnArray.add(columnName);
        }
        parameterObject.add("column", columnArray);

        parameterObject.addProperty("username", "oracle用户名");
        parameterObject.addProperty("password", "oracle密码");
        parameterObject.addProperty("splitPk", "oracle分片列");

        return parameterObject;
    }

    private static JsonObject generateMySQLWriterObject(ResultSet resultSet) throws SQLException {
        JsonObject mysqlWriterObject = new JsonObject();
        mysqlWriterObject.addProperty("name", "mysqlwriter");
        mysqlWriterObject.add("parameter", generateMySQLWriterParameter(resultSet));
        return mysqlWriterObject;
    }

    private static JsonObject generateMySQLWriterParameter(ResultSet resultSet) throws SQLException {
        JsonObject parameterObject = new JsonObject();

        JsonArray columnArray = new JsonArray();
        while (resultSet.next()) {
            String columnName = resultSet.getString("COLUMN_NAME");
            columnArray.add(columnName);
        }
        parameterObject.add("column", columnArray);

        parameterObject.addProperty("username", "mysql用户名");
        parameterObject.addProperty("password", "mysql密码");
        parameterObject.addProperty("preSql", "mysql前置SQL");
        parameterObject.addProperty("postSql", "mysql后置SQL");

        return parameterObject;
    }

    private static void writeDataXConfigToFile(JsonObject dataxConfig, String filePath) throws IOException {
        try (FileWriter fileWriter = new FileWriter(filePath)) {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            gson.toJson(dataxConfig, fileWriter);
        }
    }

    private static void generateMySQLTableDDL(ResultSet resultSet, String filePath) throws SQLException, IOException {
        try (FileWriter fileWriter = new FileWriter(filePath)) {
            StringBuilder ddlBuilder = new StringBuilder();

            ddlBuilder.append("CREATE TABLE IF NOT EXISTS mysql_table (\n");

            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME").toLowerCase(); // 将列名转换为小写
                String dataType = resultSet.getString("DATA_TYPE");
                int columnSize = resultSet.getInt("COLUMN_SIZE");
                int decimalDigits = resultSet.getInt("DECIMAL_DIGITS");

                String ddlLine = columnName + " " + getMySQLDataType(dataType, columnSize, decimalDigits) + ",\n";
                ddlBuilder.append(ddlLine);
            }

            // 添加主键信息
            String primaryKeyDDL = getPrimaryKeyDDL(resultSet);
            if (!primaryKeyDDL.isEmpty()) {
                ddlBuilder.append(primaryKeyDDL);
            }

            ddlBuilder.deleteCharAt(ddlBuilder.lastIndexOf(","));
            ddlBuilder.append(");");

            fileWriter.write(ddlBuilder.toString());
        }
    }

    private static String getMySQLDataType(String oracleDataType, int columnSize, int decimalDigits) {
        if (oracleDataType.equalsIgnoreCase("VARCHAR2") || oracleDataType.equalsIgnoreCase("NVARCHAR2")) {
            return "VARCHAR(" + columnSize + ")";
        } else if (oracleDataType.equalsIgnoreCase("CHAR") || oracleDataType.equalsIgnoreCase("NCHAR")) {
            return "CHAR(" + columnSize + ")";
        } else if (oracleDataType.equalsIgnoreCase("NUMBER")) {
            if (decimalDigits == 0) {
                if (columnSize <= 2) {
                    return "TINYINT";
                } else if (columnSize <= 4) {
                    return "SMALLINT";
                } else if (columnSize <= 9) {
                    return "INT";
                } else if (columnSize <= 18) {
                    return "BIGINT";
                } else {
                    return "DECIMAL(" + columnSize + ")";
                }
            } else {
                return "DECIMAL(" + columnSize + "," + decimalDigits + ")";
            }
        } else if (oracleDataType.equalsIgnoreCase("DATE") || oracleDataType.equalsIgnoreCase("TIMESTAMP")) {
            return "DATETIME";
        } else if (oracleDataType.equalsIgnoreCase("BLOB") || oracleDataType.equalsIgnoreCase("BFILE")) {
            return "LONGBLOB";
        } else if (oracleDataType.equalsIgnoreCase("CLOB") || oracleDataType.equalsIgnoreCase("NCLOB")) {
            return "LONGTEXT";
        } else if (oracleDataType.equalsIgnoreCase("RAW") || oracleDataType.equalsIgnoreCase("LONG RAW") ||
                oracleDataType.equalsIgnoreCase("BINARY_FLOAT") || oracleDataType.equalsIgnoreCase("BINARY_DOUBLE")) {
            return "BINARY";
        } else {
            return "VARCHAR(255)";
        }
    }

    private static String getPrimaryKeyDDL(ResultSet resultSet) throws SQLException {
        StringBuilder primaryKeyBuilder = new StringBuilder();

        DatabaseMetaData metaData = resultSet.getStatement().getConnection().getMetaData();
        ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, resultSet.getString("TABLE_NAME"));

        while (primaryKeys.next()) {
            String columnName = primaryKeys.getString("COLUMN_NAME").toLowerCase(); // 将列名转换为小写
            primaryKeyBuilder.append(columnName).append(",");
        }

        if (primaryKeyBuilder.length() > 0) {
            primaryKeyBuilder.insert(0, "PRIMARY KEY (");
            primaryKeyBuilder.setCharAt(primaryKeyBuilder.length() - 1, ')');
            primaryKeyBuilder.append(",");
        }

        return primaryKeyBuilder.toString();
    }
}
