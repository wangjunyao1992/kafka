package com.wjy.serializer;

import lombok.*;

import java.io.Serializable;
import java.util.Date;

/**
 * @author wangjunyao
 * @version 1.0.0
 * @Description TODO
 * @createTime 2021年01月05日 18:34:00
 */
@Getter
@Setter
@ToString
@AllArgsConstructor
public class User implements Serializable {

    private Integer id;

    private String name;

    private Date birthday;

}
