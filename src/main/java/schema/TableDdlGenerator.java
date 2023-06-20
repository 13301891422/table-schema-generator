package schema;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.log.level.Level;
import com.alibaba.druid.pool.DruidDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class TableDdlGenerator {

    public static String filePath;
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        GenerateFlinkSqlSchema.initHutoolDbConfig(false, Level.ERROR);

        try (DruidDataSource dataSource = GenerateFlinkSqlSchema.buildDataSource(parameters)) {
//            String tableName = "HS_ORD_ORDER";
            filePath = parameters.get("file.path");
            String tableName = parameters.get("oracle.table");
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
                " A.TABLE_NAME = ? ", tableName);

            StringBuilder tidbDdlBuilder = new StringBuilder();
            StringBuilder starRocksDdlBuilder = new StringBuilder();
            tidbDdlBuilder.append("CREATE TABLE ").append(tableName.toLowerCase()).append(" (\n");
            starRocksDdlBuilder.append("CREATE TABLE ").append(tableName.toLowerCase()).append(" (\n");

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

                tidbDdlBuilder.append(" ").append(String.format("%-24s", columnName))
                        .append("\t")
                        .append(tidbDataType)
                        .append("N".equalsIgnoreCase(nullable) ? " NOT NULL" : "")
                        .append("\t").append("COMMENT '").append(comments).append("'")
                        .append(",")
                        .append("\n");

                starRocksDdlBuilder.append(" ").append(String.format("%-24s", columnName))
                        .append("\t")
                        .append(starRocksDataType)
                        .append("N".equalsIgnoreCase(nullable) ? " NOT NULL" : "")
                        .append("\t").append("COMMENT '").append(comments).append("'")
                        .append(",")
                        .append("\n");
            }

            starRocksDdlBuilder.deleteCharAt(starRocksDdlBuilder.lastIndexOf(","));

            List<Entity> primaryKeys = Db.use(dataSource)
                    .query("SELECT COLUMN_NAME FROM USER_CONSTRAINTS uc JOIN USER_CONS_COLUMNS ucc ON uc.CONSTRAINT_NAME = " +
                            "ucc.CONSTRAINT_NAME WHERE uc.TABLE_NAME = ? AND uc.CONSTRAINT_TYPE = 'P'", tableName);

            String primaryKeyDdl = getPrimarykeyDdl(primaryKeys, tableName);
//            String uniqueIndexDDL = getUniqueIndexDDL(dataSource, tableName);

            if (StringUtils.isNotBlank(primaryKeyDdl)) {
                tidbDdlBuilder.append(primaryKeyDdl);
            }

            tidbDdlBuilder.append("\n) ENGINE = InnoDB DEFAULT CHARSET=utf8mb4;");


            starRocksDdlBuilder.append("\n) ENGINE = OLAP\n")
                    .append(primaryKeyDdl).append("\n")
                    .append("DISTRIBUTED BY HASH(具体Hash字段) BUCKETS 具体分桶数量");

            String tidbDdl = tidbDdlBuilder.toString();
            String starRocksDdl = starRocksDdlBuilder.toString();

//            System.out.println(".................Tidb Table DDL.................");
//            System.out.println(tidbDdl);
//
//            System.out.println(".................StarRocks Table DDL.................");
//            System.out.println(starRocksDdl);

//            StringBuilder finalBuilder = new StringBuilder();
//            finalBuilder.append(".................Tidb Table DDL.................")


//            writeDDLToFile(tidbDDL, "tidb_table.sql");
//            writeDDLToFile(starRocksDDL, "starrocks_table.sql");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String getTidbDataType(String oracleDataType, Integer columnSize,
                                          Integer dataPrecision, Integer dataScale) {
        if ("NUMBER".equalsIgnoreCase(oracleDataType)) {

            if (dataScale == null) {
                return "BIGINT";
            }

            if (dataScale > 0) {
                // 小数点后还有值，类似于NUMBER(10,2这种的)
                if (dataPrecision > 0) {
                    return "DECIMAL(" + dataPrecision + ", " + dataScale + ")";
                } else {
                    // 
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
        } else if ("VARCHAR2".equalsIgnoreCase(oracleDataType) || "NVARCHAR2".equalsIgnoreCase(oracleDataType)) {
            if (columnSize > 0 && columnSize <= 65535) {
                return "VARCHAR(" + columnSize + ")";
            } else if (columnSize > 65535) {
                return "TEXT";
            } else {
                return "VARCHAR(255)";
            }
        } else if ("CHAR".equalsIgnoreCase(oracleDataType) || "NCHAR".equalsIgnoreCase(oracleDataType)) {
            if (columnSize > 0 && columnSize <= 255) {
                return "CHAR(" + columnSize + ")";
            } else {
                return "CHAR(1)";
            }
        } else if ("DATE".equalsIgnoreCase(oracleDataType)) {
            return "DATETIME";
        } else if ("TIMESTAMP".equalsIgnoreCase(oracleDataType)) {
            return "TIMESTAMP";
        } else if ("CLOB".equalsIgnoreCase(oracleDataType)) {
            return "TEXT";
        } else if ("BLOB".equalsIgnoreCase(oracleDataType)) {
            return "BLOB";
        } else {
            return "TEXT";
        }
    }

    private static String getStarRocksDataType(String oracleDataType, Integer columnSize, Integer dataPrecision, Integer dataScale) {
        if ("NUMBER".equalsIgnoreCase(oracleDataType)) {
            if (dataScale == null) {
                return "BIGINT";
            }
            if (dataScale > 0) {
                if (dataPrecision > 0) {
//                    if (dataPrecision <= 9) {
                    return "DECIMAL(" + dataPrecision + ", " + dataScale + ")";
//                    } else {
//                        return "DECIMAL(" + dataPrecision + ", 0)";
//                    }
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
        } else if ("VARCHAR2".equalsIgnoreCase(oracleDataType) || "NVARCHAR2".equalsIgnoreCase(oracleDataType)) {
            if (columnSize > 0 && columnSize <= 65535) {
                return "VARCHAR(" + columnSize * 4 + ")";
            } else if (columnSize > 65535) {
                return "TEXT";
            } else {
                return "VARCHAR(255)";
            }
        } else if ("CHAR".equalsIgnoreCase(oracleDataType) || "NCHAR".equalsIgnoreCase(oracleDataType)) {
            if (columnSize > 0 && columnSize <= 255) {
                return "CHAR(" + columnSize + ")";
            } else {
                return "CHAR(1)";
            }
        } else if ("DATE".equalsIgnoreCase(oracleDataType)) {
            return "DATETIME";
        } else if ("TIMESTAMP".equalsIgnoreCase(oracleDataType)) {
            return "DATETIME";
        } else if ("CLOB".equalsIgnoreCase(oracleDataType)) {
            return "STRING";
        } else if ("BLOB".equalsIgnoreCase(oracleDataType)) {
            return "BINARY";
        } else {
            return "STRING";
        }
    }


    private static String getPrimarykeyDdl(List<Entity> primaryKeys, String tableName) throws SQLException {

        if (CollUtil.isNotEmpty(primaryKeys)) {
            StringBuilder primaryKeyDdl = new StringBuilder();
            primaryKeyDdl.append("PRIMARY KEY (");

            for (Entity primaryKey : primaryKeys) {
                primaryKeyDdl.append(primaryKey.getStr("COLUMN_NAME").toLowerCase()).append(",");
            }
            return primaryKeyDdl.deleteCharAt(primaryKeyDdl.lastIndexOf(",")).append(")").toString();
        }

        System.out.println("表 : " + tableName + " 不存在主键 , 请根据需要指定主键字段");

        return "";
    }

    private static String getUniqueIndexDdl(DruidDataSource dataSource, String tableName) throws SQLException {
        List<Entity> uniqueIndexes = Db
                .use(dataSource)
                .query(" SELECT COLUMN_NAME FROM USER_INDEXES ui JOIN USER_IND_COLUMNS uic ON ui.INDEX_NAME = uic.INDEX_NAME " +
                        " WHERE ui.TABLE_NAME = ? AND ui.UNIQUENESS = 'UNIQUE'", tableName);

        if (!uniqueIndexes.isEmpty()) {
            StringBuilder uniqueIndexDdl = new StringBuilder();

            for (Entity uniqueIndex : uniqueIndexes) {
                uniqueIndexDdl.append(",\nUNIQUE KEY ").append(uniqueIndex.getStr("COLUMN_NAME").toLowerCase()).append(" (").append(uniqueIndex.getStr("COLUMN_NAME").toLowerCase()).append(")");
            }

            return uniqueIndexDdl.toString();
        }

        return "";
    }

    private static void writeDdlToFile(String ddl, String filePath) {
        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write(ddl);
            System.out.println("Table DDL has been written to file: " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
