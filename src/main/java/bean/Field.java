package bean;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
public class Field implements Serializable {

    private static final long serialVersionUID = -1122332142027453248L;
    private String fieldName;
    private String fieldType;
    private String comments;
    private Long fieldLength;
}
