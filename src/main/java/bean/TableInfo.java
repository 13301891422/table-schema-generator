package bean;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.compress.utils.Lists;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@ToString
public class TableInfo implements Serializable {
    private static final long serialVersionUID = -1122332442077451248L;

    private String table;
    private String database;
    private List<Field> columns = Lists.newArrayList();
    private List<String> primaryKey = Lists.newArrayList();
    private List<String> uniqueKey = Lists.newArrayList();
}
