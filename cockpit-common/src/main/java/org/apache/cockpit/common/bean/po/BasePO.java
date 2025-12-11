package org.apache.cockpit.common.bean.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;
import org.apache.cockpit.common.utils.IdWorkerUtil;

import java.io.Serializable;
import java.util.Date;

@Data
public class BasePO implements Serializable {
    /**
     * 主键
     */
    @TableId(type = IdType.ASSIGN_UUID)
    protected String id;

    /**
     * 创建时间
     */
    protected Date createTime;

    /**
     * 创建人
     */
    protected String createBy;

    /**
     * 更新时间
     */
    protected Date updateTime;

    /**
     * 更新人
     */
    protected String updateBy;

    /**
     * 初始化更新信息
     */
    public void initUpdate() {
        this.updateTime = new Date();
    }


    /**
     * 初始化插入信息
     */
    public void initInsert() {
        this.createBy = "sys";
        this.updateBy = "sys";
        this.id = IdWorkerUtil.get32UUID();

        this.createTime = new Date();
        this.updateTime = new Date();
    }


    /**
     * 初始化插入信息
     */
    public void initInsertWithNoId() {
        this.createBy = "sys";
        this.updateBy = "sys";

        this.createTime = new Date();
        this.updateTime = new Date();
    }
}
