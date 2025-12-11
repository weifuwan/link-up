package org.apache.cockpit.connectors.hive3.config;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


@Data
public class HadoopConf implements Serializable {
    private static final String HDFS_IMPL = "org.apache.hadoop.hdfs.DistributedFileSystem";
    private static final String SCHEMA = "hdfs";
    protected Map<String, String> extraOptions = new HashMap<>();
    protected String hdfsNameKey;
    protected String hdfsSitePath;

    protected String remoteUser;

    private String krb5Path;
    protected String kerberosPrincipal;
    protected String kerberosKeytabPath;

    public HadoopConf(String hdfsNameKey) {
        this.hdfsNameKey = hdfsNameKey;
    }

    public String getFsHdfsImpl() {
        return HDFS_IMPL;
    }

    public String getSchema() {
        return SCHEMA;
    }

    public void setExtraOptionsForConfiguration(Configuration configuration) {
        if (!extraOptions.isEmpty()) {
            removeUnwantedOverwritingProps(extraOptions);
            extraOptions.forEach(configuration::set);
        }
        if (StringUtils.isNotBlank(hdfsSitePath)) {
            Configuration hdfsSiteConfiguration = new Configuration();
            hdfsSiteConfiguration.addResource(new Path(hdfsSitePath));
            unsetUnwantedOverwritingProps(hdfsSiteConfiguration);
            configuration.addResource(hdfsSiteConfiguration);
        }
    }

    private void removeUnwantedOverwritingProps(Map extraOptions) {
        extraOptions.remove(getFsDefaultNameKey());
        extraOptions.remove(getHdfsImplKey());
        extraOptions.remove(getHdfsImplDisableCacheKey());
    }

    public void unsetUnwantedOverwritingProps(Configuration hdfsSiteConfiguration) {
        hdfsSiteConfiguration.unset(getFsDefaultNameKey());
        hdfsSiteConfiguration.unset(getHdfsImplKey());
        hdfsSiteConfiguration.unset(getHdfsImplDisableCacheKey());
    }

    public Configuration toConfiguration() {
        Configuration configuration = new Configuration();
//        configuration.setBoolean(READ_INT96_AS_FIXED, true);
//        configuration.setBoolean(ADD_LIST_ELEMENT_RECORDS, false);
//        configuration.setBoolean(WRITE_OLD_LIST_STRUCTURE, true);
        configuration.setBoolean(getHdfsImplDisableCacheKey(), true);
        configuration.set(getFsDefaultNameKey(), getHdfsNameKey());
        configuration.set(getHdfsImplKey(), getFsHdfsImpl());
        return configuration;
    }

    public String getFsDefaultNameKey() {
        return CommonConfigurationKeys.FS_DEFAULT_NAME_KEY;
    }

    public String getHdfsImplKey() {
        return String.format("fs.%s.impl", getSchema());
    }

    public String getHdfsImplDisableCacheKey() {
        return String.format("fs.%s.impl.disable.cache", getSchema());
    }
}
