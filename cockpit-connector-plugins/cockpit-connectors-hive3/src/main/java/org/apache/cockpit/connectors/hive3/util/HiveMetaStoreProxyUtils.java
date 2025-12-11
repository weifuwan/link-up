package org.apache.cockpit.connectors.hive3.util;

import lombok.experimental.UtilityClass;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.hive3.config.FileBaseSourceOptions;

@UtilityClass
public class HiveMetaStoreProxyUtils {

    public boolean enableKerberos(ReadonlyConfig config) {
        boolean kerberosPrincipalEmpty =
                config.getOptional(FileBaseSourceOptions.KERBEROS_PRINCIPAL).isPresent();
        boolean kerberosKeytabPathEmpty =
                config.getOptional(FileBaseSourceOptions.KERBEROS_KEYTAB_PATH).isPresent();
        if (kerberosKeytabPathEmpty && kerberosPrincipalEmpty) {
            return true;
        }
        if (!kerberosPrincipalEmpty && !kerberosKeytabPathEmpty) {
            return false;
        }
        if (kerberosPrincipalEmpty) {
            throw new IllegalArgumentException("Please set kerberosPrincipal");
        }
        throw new IllegalArgumentException("Please set kerberosKeytabPath");
    }

    public boolean enableRemoteUser(ReadonlyConfig config) {
        return config.getOptional(FileBaseSourceOptions.REMOTE_USER).isPresent();
    }
}
