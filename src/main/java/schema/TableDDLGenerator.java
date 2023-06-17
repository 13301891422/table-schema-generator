package schema;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.Session;
import cn.hutool.db.SqlRunner;
import cn.hutool.log.level.Level;
import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.base.MoreObjects;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class TableDDLGenerator {
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        GenerateFlinkSqlSchema.initHutoolDbConfig(false, Level.ERROR);

        try (DruidDataSource dataSource = GenerateFlinkSqlSchema.buildDataSource(parameters)) {
            String tableName = "HS_PROBLEM_SITE";
            List<Entity> columns = Db
                    .use(dataSource)
                    .query(" " +
                            " SELECT\n" +
                            "    A.COLUMN_NAME, A.DATA_TYPE, A.DATA_LENGTH, A.DATA_PRECISION, A.DATA_SCALE, A.NULLABLE, B.COMMENTS\n" +
                            "    FROM USER_TAB_COLUMNS A\n" +
                            " LEFT JOIN USER_COL_COMMENTS B\n" +
                            " ON A.TABLE_NAME = B.TABLE_NAME\n" +
                            " AND A.COLUMN_NAME = B.COLUMN_NAME\n" +
                            " WHERE\n" +
                            "    A.TABLE_NAME = ? ", tableName);


            StringBuilder tidbDDLBuilder = new StringBuilder();
            StringBuilder starRocksDDLBuilder = new StringBuilder();
            tidbDDLBuilder.append("CREATE TABLE ").append(tableName.toLowerCase()).append(" (\n");
            starRocksDDLBuilder.append("CREATE TABLE ").append(tableName.toLowerCase()).append(" (\n");

            for (Entity column : columns) {
                String columnName = column.getStr("COLUMN_NAME").toLowerCase();
                String oracleDataType = column.getStr("DATA_TYPE");
                Integer columnSize = column.getInt("DATA_LENGTH");
                Integer dataPrecision = column.getInt("DATA_PRECISION");
                Integer dataScale = column.getInt("DATA_SCALE");
                String nullable = column.getStr("NULLABLE");
                String comments = StringUtils.isBlank(column.getStr("COMMENTS")) ? "" : column.getStr("COMMENTS");

                String tidbDataType = getTidbDataType(oracleDataType, columnSize, dataPrecision, dataScale);
                String starRocksDataType = getStarRocksDataType(oracleDataType, columnSize, dataPrecision, dataScale);

                tidbDDLBuilder.append(" ").append(String.format("%-24s", columnName))
                        .append("\t")
                        .append(tidbDataType)
                        .append(nullable.equalsIgnoreCase("N") ? " NOT NULL" : "")
                        .append("\t").append("COMMENT '").append(comments).append("'")
                        .append(",")
                        .append("\n");

                starRocksDDLBuilder.append(" ").append(String.format("%-24s", columnName))
                        .append("\t")
                        .append(starRocksDataType)
                        .append(nullable.equalsIgnoreCase("N") ? " NOT NULL" : "")
                        .append("\t").append("COMMENT '").append(comments).append("'")
                        .append(",")
                        .append("\n");

                starRocksDDLBuilder.deleteCharAt(starRocksDDLBuilder.lastIndexOf(","));
            }

            List<Entity> primaryKeys = Db.use(dataSource)
                    .query("SELECT COLUMN_NAME FROM USER_CONSTRAINTS uc JOIN USER_CONS_COLUMNS ucc ON uc.CONSTRAINT_NAME = " +
                            "ucc.CONSTRAINT_NAME WHERE uc.TABLE_NAME = ? AND uc.CONSTRAINT_TYPE = 'P'", tableName);

            String primaryKeyDDL = getPrimaryKeyDDL(primaryKeys, tableName);
//            String uniqueIndexDDL = getUniqueIndexDDL(dataSource, tableName);

            if (StringUtils.isNotBlank(primaryKeyDDL)) {
                tidbDDLBuilder.append(primaryKeyDDL);
            }

            tidbDDLBuilder.append("\n) ENGINE = InnoDB DEFAULT CHARSET=utf8mb4;");


            starRocksDDLBuilder.append("\n) ENGINE = OLAP\n")
                    .append(primaryKeyDDL).append("\n")
                    .append("DISTRIBUTED BY HASH(具体Hash字段) BUCKETS 具体分桶数量")
            ;

            String tidbDDL = tidbDDLBuilder.toString();
            String starRocksDDL = starRocksDDLBuilder.toString();

            System.out.println(".................Tidb Table DDL.................");
            System.out.println(tidbDDL);

            System.out.println(".................StarRocks Table DDL.................");
            System.out.println(starRocksDDL);

//            writeDDLToFile(tidbDDL, "tidb_table.sql");
//            writeDDLToFile(starRocksDDL, "starrocks_table.sql");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String getTidbDataType(String oracleDataType, Integer columnSize, Integer dataPrecision, Integer dataScale) {
        if (oracleDataType.equalsIgnoreCase("NUMBER")) {
            if (dataScale > 0) {
                if (dataPrecision > 0) {
                    if (dataPrecision <= 9) {
                        return "DECIMAL(" + dataPrecision + ", " + dataScale + ")";
                    } else {
                        return "DECIMAL(" + dataPrecision + ", 0)";
                    }
                } else {
                    return "DECIMAL(65, " + dataScale + ")";
                }
            } else {
                if (dataPrecision > 0) {
                    if (dataPrecision <= 3) {
                        return "TINYINT";
                    } else if (dataPrecision <= 5) {
                        return "SMALLINT";
                    } else if (dataPrecision <= 9) {
                        return "INT";
                    } else if (dataPrecision <= 18) {
                        return "BIGINT";
                    } else {
                        return "DECIMAL(" + dataPrecision + ", 0)";
                    }
                } else {
                    return "BIGINT";
                }
            }
        } else if (oracleDataType.equalsIgnoreCase("VARCHAR2") || oracleDataType.equalsIgnoreCase("NVARCHAR2")) {
            if (columnSize > 0 && columnSize <= 65535) {
                return "VARCHAR(" + columnSize + ")";
            } else if (columnSize > 65535) {
                return "TEXT";
            } else {
                return "VARCHAR(255)";
            }
        } else if (oracleDataType.equalsIgnoreCase("CHAR") || oracleDataType.equalsIgnoreCase("NCHAR")) {
            if (columnSize > 0 && columnSize <= 255) {
                return "CHAR(" + columnSize + ")";
            } else {
                return "CHAR(1)";
            }
        } else if (oracleDataType.equalsIgnoreCase("DATE")) {
            return "DATETIME";
        } else if (oracleDataType.equalsIgnoreCase("TIMESTAMP")) {
            return "TIMESTAMP";
        } else if (oracleDataType.equalsIgnoreCase("CLOB")) {
            return "TEXT";
        } else if (oracleDataType.equalsIgnoreCase("BLOB")) {
            return "BLOB";
        } else {
            return "TEXT";
        }
    }

    private static String getStarRocksDataType(String oracleDataType, Integer columnSize, Integer dataPrecision, Integer dataScale) {
        if (oracleDataType.equalsIgnoreCase("NUMBER")) {
            if (dataScale > 0) {
                if (dataPrecision > 0) {
                    if (dataPrecision <= 9) {
                        return "DECIMAL(" + dataPrecision + ", " + dataScale + ")";
                    } else {
                        return "DECIMAL(" + dataPrecision + ", 0)";
                    }
                } else {
                    return "DECIMAL(18, " + dataScale + ")";
                }
            } else {
                if (dataPrecision > 0) {
                    if (dataPrecision <= 3) {
                        return "TINYINT";
                    } else if (dataPrecision <= 5) {
                        return "SMALLINT";
                    } else if (dataPrecision <= 9) {
                        return "INT";
                    } else if (dataPrecision <= 18) {
                        return "BIGINT";
                    } else {
                        return "DECIMAL(" + dataPrecision + ", 0)";
                    }
                } else {
                    return "BIGINT";
                }
            }
        } else if (oracleDataType.equalsIgnoreCase("VARCHAR2") || oracleDataType.equalsIgnoreCase("NVARCHAR2")) {
            if (columnSize > 0 && columnSize <= 65535) {
                return "VARCHAR(" + columnSize + ")";
            } else if (columnSize > 65535) {
                return "TEXT";
            } else {
                return "VARCHAR(255)";
            }
        } else if (oracleDataType.equalsIgnoreCase("CHAR") || oracleDataType.equalsIgnoreCase("NCHAR")) {
            if (columnSize > 0 && columnSize <= 255) {
                return "CHAR(" + columnSize + ")";
            } else {
                return "CHAR(1)";
            }
        } else if (oracleDataType.equalsIgnoreCase("DATE")) {
            return "DATETIME";
        } else if (oracleDataType.equalsIgnoreCase("TIMESTAMP")) {
            return "DATETIME";
        } else if (oracleDataType.equalsIgnoreCase("CLOB")) {
            return "STRING";
        } else if (oracleDataType.equalsIgnoreCase("BLOB")) {
            return "BINARY";
        } else {
            return "STRING";
        }
    }


    private static String getPrimaryKeyDDL(List<Entity> primaryKeys, String tableName) throws SQLException {

        if (CollUtil.isNotEmpty(primaryKeys)) {
            StringBuilder primaryKeyDDL = new StringBuilder();
            primaryKeyDDL.append("PRIMARY KEY (");

            for (Entity primaryKey : primaryKeys) {
                primaryKeyDDL.append(primaryKey.getStr("COLUMN_NAME").toLowerCase()).append(",");
            }
            return primaryKeyDDL.deleteCharAt(primaryKeyDDL.lastIndexOf(",")).append(")").toString();
        }

        System.out.println("表 : " + tableName + " 不存在主键 , 请根据需要指定主键字段");

        return "";
    }

    private static String getUniqueIndexDDL(DruidDataSource dataSource, String tableName) throws SQLException {
        List<Entity> uniqueIndexes = Db
                .use(dataSource)
                .query(" SELECT COLUMN_NAME FROM USER_INDEXES ui JOIN USER_IND_COLUMNS uic ON ui.INDEX_NAME = uic.INDEX_NAME " +
                        " WHERE ui.TABLE_NAME = ? AND ui.UNIQUENESS = 'UNIQUE'", tableName);

        if (!uniqueIndexes.isEmpty()) {
            StringBuilder uniqueIndexDDL = new StringBuilder();

            for (Entity uniqueIndex : uniqueIndexes) {
                uniqueIndexDDL.append(",\nUNIQUE KEY ").append(uniqueIndex.getStr("COLUMN_NAME").toLowerCase()).append(" (").append(uniqueIndex.getStr("COLUMN_NAME").toLowerCase()).append(")");
            }

            return uniqueIndexDDL.toString();
        }

        return "";
    }

    private static void writeDDLToFile(String ddl, String filePath) {
        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write(ddl);
            System.out.println("Table DDL has been written to file: " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
