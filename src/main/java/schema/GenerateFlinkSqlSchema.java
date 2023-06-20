package schema;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.GlobalDbConfig;
import cn.hutool.log.level.Level;
import com.alibaba.druid.pool.DruidDataSource;
import bean.Field;
import bean.TableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

/***
 * 读取Oracle指定表元数据，生成FlinkSql的Source、Sink、InsertIntoStatement语句
 * @author weiwei
 */
public class GenerateFlinkSqlSchema {

    public static String filePath;

    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromArgs(args);
        initHutoolDbConfig(false, Level.ERROR);
        DruidDataSource dataSource = buildDataSource(parameters);
        try {
            String table = parameters.get("oracle.table");
            filePath = parameters.get("file.path");
//        String database = parameters.get("oracle.database");
            String columnsQuerySql = getColumnsQuerySql(table);
            String priUniqQuerySql = getPriAndUniqKeySql(table);
            TableInfo tableInfo = new TableInfo();
            List<Field> columns = tableInfo.getColumns();
            List<String> primaryKeys = tableInfo.getPrimaryKey();
            List<String> uniqueKeys = tableInfo.getUniqueKey();

            // 查询表字段信息
            List<Entity> columnsResult = Db.use(dataSource).query(columnsQuerySql);
            buildColumns(columnsResult, columns, table);

            // 查询主键和唯一建信息
            List<Entity> uniqAndPriResult = Db.use(dataSource).query(priUniqQuerySql);
            buildPriAndUniqKey(uniqAndPriResult, primaryKeys, uniqueKeys);

            String tableMessage = String.format("--数据库名为:%s, 表名为:%s", parameters.get("source.database"), table);
            String keyMessage = String.format("--主键字段为:%s, 唯一索引字段为:%s, 该表的总列数为:%s", tableInfo.getPrimaryKey().toString(), tableInfo.getUniqueKey().toString(), tableInfo.getColumns().size());

            buildCreateTableStatement(columns, primaryKeys, uniqueKeys, table.toLowerCase(Locale.ROOT), tableMessage, keyMessage);
        } finally {
            try {
                dataSource.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static String getPriAndUniqKeySql(String table) {
        return String.format("" + " SELECT " + "    " +
                " CASE WHEN AU.CONSTRAINT_TYPE = 'P' THEN CU.COLUMN_NAME ELSE '' END PRIMARY_KEY, " + "     " +
                " CASE WHEN AU.CONSTRAINT_TYPE = 'U' THEN CU.COLUMN_NAME ELSE '' END UNIQUE_KEY   " + " FROM " +
                " USER_CONS_COLUMNS CU,USER_CONSTRAINTS AU                                       " + " " +
                " WHERE CU.CONSTRAINT_NAME = AU.CONSTRAINT_NAME AND AU.TABLE_NAME = '%s' " + " " +
                " AND AU.CONSTRAINT_TYPE IN ('P','U')", table);
    }

    public static String getColumnsQuerySql(String table) {
        return String.format("" +
                " SELECT LOWER(A.COLUMN_NAME) || ' ' AS FIELD_NAME,\n" +
                " CASE  WHEN A.DATA_TYPE = 'NUMBER' AND A.DATA_SCALE = 0 AND A.DATA_PRECISION < 5 THEN 'INTEGER'\n" +
                " WHEN A.DATA_TYPE = 'NUMBER' AND A.DATA_SCALE = 0 AND A.DATA_PRECISION >= 5 AND A.DATA_PRECISION <= 9 THEN 'INTEGER'\n" +
                " WHEN A.DATA_TYPE = 'NUMBER' AND A.DATA_SCALE = 0 AND A.DATA_PRECISION >= 9 AND A.DATA_PRECISION <= 18 THEN 'BIGINT'\n" +
                " WHEN A.DATA_TYPE = 'NUMBER' AND A.DATA_PRECISION > 18 THEN 'STRING'\n" +
                " WHEN A.DATA_TYPE = 'NUMBER' AND A.DATA_SCALE >= 1 THEN 'STRING'\n" +
                " WHEN A.DATA_TYPE = 'VARCHAR2' THEN 'STRING'\n" +
                " WHEN A.DATA_TYPE = 'DATE' THEN 'STRING'\n" +
                " WHEN A.DATA_TYPE = 'TIMESTAMP' THEN 'STRING'\n" +
                " ELSE 'STRING'             END                     AS DATA_TYPE,\n" +
                " B.comments                  AS DATA_COMMENT\n" +
                " FROM USER_TAB_COLUMNS A\n" +
                " LEFT JOIN user_col_comments B ON A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME\n" +
                " WHERE A.TABLE_NAME = '%s'  ORDER BY A.COLUMN_ID", table);
    }

    public static void buildColumns(List<Entity> columnsResult, List<Field> columns, String tableName) {
        columnsResult.forEach(entity -> {
            Field field = new Field();
            String columnName = entity.getStr("FIELD_NAME");
            String comments = entity.getStr("DATA_COMMENT");
            String dataType = entity.getStr("DATA_TYPE");
            field.setFieldName(columnName);
            field.setComments(comments);
            field.setFieldType(dataType);
            columns.add(field);
        });
        if (CollUtil.isEmpty(columnsResult)) {
            throw new IllegalArgumentException("查询的Oracle表不存在... 输入的表名为:" + tableName);
        }
    }

    public static void buildPriAndUniqKey(List<Entity> uniqAndPriResult, List<String> primaryKeys, List<String> uniqueKeys) {
        uniqAndPriResult.forEach(entity -> {
            String primaryKey = entity.getStr("PRIMARY_KEY");
            String uniqueKey = entity.getStr("UNIQUE_KEY");
            if (StringUtils.isNotBlank(primaryKey)) {
                primaryKeys.add(primaryKey);
            }
            if (StringUtils.isNotBlank(uniqueKey)) {
                uniqueKeys.add(uniqueKey);
            }
        });
    }

    public static void buildCreateTableStatement(List<Field> tableColumns, List<String> primarykey, List<String> uniqueKey, String tableName, String tableMessage, String keyMessage) {
        StringBuilder fieldNameTypeBuilder = new StringBuilder();
        StringBuilder fieldNameBuilder = new StringBuilder();
        for (Field column : tableColumns) {
            String fieldName = column.getFieldName();
            String fieldType = column.getFieldType();
            fieldNameTypeBuilder.append(" ").append(String.format("%-24s", fieldName)).append("\t").append(fieldType).append(",").append("\n");
            fieldNameBuilder.append(" ").append(String.format("%-24s", fieldName)).append("\t").append(",").append("\n");
        }

        String fieldAndTypeStrWithPriKey = fieldNameTypeBuilder.toString();
        String fieldAndTypeStrNoPriKey = fieldNameTypeBuilder.deleteCharAt(fieldNameTypeBuilder.lastIndexOf(",")).toString();
        String fieldNameStr = fieldNameBuilder.deleteCharAt(fieldNameBuilder.lastIndexOf(",")).toString();

        String sourceStatement = "CREATE TABLE " + "source_" + tableName + " (" + "\n" + fieldAndTypeStrNoPriKey + ") WITH (" + "\n" + "  " + "'connector' = '${kafka.connector}'" + ",\n" + "  " + "'topic' = 'ky_data_dtd_kymain_" + tableName + "'" + ",\n" + "  " + "'properties.bootstrap.servers' = '${bootstrap.servers}'" + ",\n" + "  " + "'properties.group.id' = 'group_ky_data_dtd_kymain_" + tableName + "'" + ",\n" + "  " + "'scan.startup.mode' = '${start.up.mode}'" + ",\n" + "  " + "'format' = '${ogg.format}'" + "\n" + ");";

        String flinkPrimaryKey = null;

        // 判断oracle表是否有主键
        if (CollUtil.isNotEmpty(primarykey)) {
            flinkPrimaryKey = String.join(",", primarykey);
        } else {
            if (CollUtil.isNotEmpty(uniqueKey)) {
                flinkPrimaryKey = String.join(",", uniqueKey);
            }
        }

        String createSinkStarBaseStr;
        String createSinkTidbBaseStr;
        String sinkStarPrefix = "sink_star_";
        String sinkTidbPrefix = "sink_tidb_";

        // Sink表是否需要指定主键, 拼接PRIMARY KEY 语句
        if (StringUtils.isNotBlank(flinkPrimaryKey)) {
            createSinkStarBaseStr = "CREATE TABLE " + sinkStarPrefix + tableName + " (" + "\n" + fieldAndTypeStrWithPriKey;
            createSinkTidbBaseStr = "CREATE TABLE " + sinkTidbPrefix + tableName + " (" + "\n" + fieldAndTypeStrWithPriKey;

            String createSinkPriStr = " PRIMARY KEY (" + flinkPrimaryKey + ") NOT ENFORCED" + "\n";
            createSinkStarBaseStr = createSinkStarBaseStr + createSinkPriStr;
            createSinkTidbBaseStr = createSinkTidbBaseStr + createSinkPriStr;
        } else {
            createSinkStarBaseStr = "CREATE TABLE " + sinkStarPrefix + tableName + " (" + "\n" + fieldAndTypeStrNoPriKey;
            createSinkTidbBaseStr = "CREATE TABLE " + sinkTidbPrefix + tableName + " (" + "\n" + fieldAndTypeStrNoPriKey;
        }

        String starRocksSinkStatement = createSinkStarBaseStr +
                " ) WITH (" + "\n" + "  " +
                " 'connector' = '${starrocks_connector}'" + ",\n" + "  " +
                " 'jdbc-url' = '${starrocks_jdbc_url}'" + ",\n" + "  " +
                " 'load-url' = '${starrocks_load_url}'" + ",\n" + "  " +
                " 'database-name' = '${starrocks.database}'" + ",\n" + "  " +
                " 'table-name' = '${starrocks.table}'" + ",\n" + "  " +
                " 'username' = '${starrocks.username}'" + ",\n" + "  " +
                " 'password' = '${starrocks.password}'" + ",\n" + "  " +
                " 'sink.buffer-flush.max-rows' = '${starrocks_sink_buffer_max_rows}'" + ",\n" + "  " +
                " 'sink.buffer-flush.max-bytes' = '${starrocks_sink_buffer_flush_max_bytes}'" + ",\n" + "  " +
                " 'sink.buffer-flush.interval-ms' = '${starrocks_sink_buffer_interval_ms}',\n" + "  " +
                " 'sink.properties.column_separator' = '${starrocks_sink_properties_column_separator}'" + ",\n" + "  " +
                " 'sink.properties.row_delimiter' = '${starrocks_sink_properties_row_delimiter}'" + ",\n" + "  " +
                " 'sink.max-retries' = '${starrocks_sink_max_retries}'" + "\n" + ");";

        String tidbSinkStatement = createSinkTidbBaseStr + "" +
                " ) WITH (" + "\n" + "  " +
                " 'connector' = '${jdbc_connector}'" + ",\n" + "  " +
                " 'table-name' = 'tidb_table'" + ",\n" + "  " +
                " 'url' = '${tidb_url}'" + ",\n" + "  " +
                " 'username' = '${tidb_username}'" + ",\n" + "  " +
                " 'password' = '${tidb_password}'" + ",\n" + "  " +
                " 'sink.buffer-flush.interval' = '${tidb_sink_buffer_flush_interval}'" + ",\n" + "  " +
                " 'sink.buffer-flush.max-rows' = '${tidb_sink_buffer_flush_max_rows}'" + ",\n" + "  " +
                " 'sink.max-retries' = '${tidb_sink_max_retries}'" + "\n" +
                " );";

        String insertStarStatement = insertStatement(sinkStarPrefix, tableName, fieldNameStr);
        String insertTidbStatement = insertStatement(sinkTidbPrefix, tableName, fieldNameStr);

        String flinkSqlStr = tableMessage + "\n" + keyMessage + "\n\n" + "-----------------Source-----------------" + "\n" + sourceStatement + "\n\n" + "-----------------StarSink-----------------" + "\n" + starRocksSinkStatement + "\n\n" + "-----------------TidbSink-----------------" + "\n" + tidbSinkStatement + "\n\n" + "-----------------InsertStar-----------------" + "\n" + insertStarStatement + "\n\n" + "-----------------InsertTidb-----------------" + "\n" + insertTidbStatement;

//        System.out.println(flinkSqlStr);
        FileUtil.writeString(flinkSqlStr, filePath + "/flink_schema.txt", StandardCharsets.UTF_8);
    }

    public static String insertStatement(String sinkTablePrefix, String tableName, String fieldNameStr) {
        return "INSERT INTO " + sinkTablePrefix + tableName + " SELECT " + " * " + "FROM " + "source_" + tableName + ";";
    }

    public static DruidDataSource buildDataSource(ParameterTool parameters) throws ClassNotFoundException {
//        String database = parameters.get("oracle.database");
//        String table = parameters.get("oracle.table");
//        String driver = parameters.get("oracle.driver");
//        String url = parameters.get("oracle.url");
//        String username = parameters.get("oracle.username");
//        String password = parameters.get("oracle.password");
//        String sid = parameters.get("oracle.sid");
//        String host = parameters.get("oracle.host");

        // Oracle 配置
        String sid = "ztodbkf";
        String driver = "oracle.jdbc.OracleDriver";
        String host = "10.202.16.14";
        String url = String.format("jdbc:oracle:thin:@%s:1521:%s", host, sid);
        String username = "ztotest";
        String password = "Dyzto.#09";

        Class.forName(driver);
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setLogAbandoned(true);
        dataSource.setUrl(url);
        dataSource.setTestWhileIdle(false);
        dataSource.setInitialSize(1);
        dataSource.setTestWhileIdle(false);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        return dataSource;
    }

    public static void initHutoolDbConfig(boolean isShowSql, Level level) {
        GlobalDbConfig.setReturnGeneratedKey(false);
        GlobalDbConfig.setShowSql(isShowSql, true, true, level);
    }
}