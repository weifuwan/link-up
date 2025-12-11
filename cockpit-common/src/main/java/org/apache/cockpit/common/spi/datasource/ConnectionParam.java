package org.apache.cockpit.common.spi.datasource;

import org.apache.cockpit.common.spi.enums.DbType;

import java.io.Serializable;

/**
 * The model of Datasource Connection param
 */
public interface ConnectionParam extends Serializable {

    default String getPassword() {
        return "";
    }

    default void setPassword(String s) {
    }

    default DbType getDbType() {
        return null;
    }

    default void setDbType(DbType dbType) {
    }


}
