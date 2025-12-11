package org.apache.cockpit.connectors.elasticsearch.config;


import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

import java.io.Serializable;
import java.util.List;

public class ElasticsearchBaseOptions implements Serializable {

    public static final Option<List<String>> HOSTS =
            Options.key("hosts")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Elasticsearch cluster http address, the format is host:port, allowing multiple hosts to be specified. Such as [\"host1:9200\", \"host2:9200\"]");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("x-pack username");

    public static final Option<String> INDEX =
            Options.key("index")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Elasticsearch index name.Index support contains variables of field name,such as seatunnel_${age},and the field must appear at seatunnel row. If not, we will treat it as a normal index");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("x-pack password");

    public static final Option<Boolean> TLS_VERIFY_CERTIFICATE =
            Options.key("tls_verify_certificate")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable certificates validation for HTTPS endpoints");

    public static final Option<Boolean> TLS_VERIFY_HOSTNAME =
            Options.key("tls_verify_hostname")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable hostname validation for HTTPS endpoints");

    public static final Option<String> TLS_KEY_STORE_PATH =
            Options.key("tls_keystore_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The path to the PEM or JKS key store. This file must be readable by the operating system user running SeaTunnel.");

    public static final Option<String> TLS_KEY_STORE_PASSWORD =
            Options.key("tls_keystore_password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The key password for the key store specified");

    public static final Option<String> TLS_TRUST_STORE_PATH =
            Options.key("tls_truststore_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The path to PEM or JKS trust store. This file must be readable by the operating system user running SeaTunnel.");

    public static final Option<String> TLS_TRUST_STORE_PASSWORD =
            Options.key("tls_truststore_password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The key password for the trust store specified");
}
