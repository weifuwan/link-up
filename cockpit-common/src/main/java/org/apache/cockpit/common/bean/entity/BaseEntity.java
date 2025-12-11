package org.apache.cockpit.common.bean.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * @author bruce
 */
@Data
public class BaseEntity implements Serializable {
    protected Date createTime;

    protected Date updateTime;
}
