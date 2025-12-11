package org.apache.cockpit.connectors.influxdb.client;

import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.cockpit.connectors.influxdb.config.InfluxDBConfig;
import org.apache.cockpit.connectors.influxdb.config.SinkConfig;
import org.apache.cockpit.connectors.influxdb.exception.InfluxdbConnectorErrorCode;
import org.apache.cockpit.connectors.influxdb.exception.InfluxdbConnectorException;
import org.apache.commons.lang3.StringUtils;
import org.influxdb.InfluxDB;
import org.influxdb.impl.InfluxDBImpl;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class InfluxDBClient {
    public static InfluxDB getInfluxDB(InfluxDBConfig config) throws ConnectException {
        OkHttpClient.Builder clientBuilder =
                new OkHttpClient.Builder()
                        .connectTimeout(config.getConnectTimeOut(), TimeUnit.MILLISECONDS)
                        .readTimeout(config.getQueryTimeOut(), TimeUnit.SECONDS);
        InfluxDB.ResponseFormat format = InfluxDB.ResponseFormat.valueOf(config.getFormat());
        clientBuilder.addInterceptor(
                new Interceptor() {
                    @Override
                    public Response intercept(Chain chain) throws IOException {
                        Request request = chain.request();
                        HttpUrl httpUrl =
                                request.url()
                                        .newBuilder()
                                        // set epoch
                                        .addQueryParameter("epoch", config.getEpoch())
                                        .build();
                        Request build = request.newBuilder().url(httpUrl).build();
                        return chain.proceed(build);
                    }
                });
        InfluxDB influxdb =
                new InfluxDBImpl(
                        config.getUrl(),
                        StringUtils.isEmpty(config.getUsername())
                                ? StringUtils.EMPTY
                                : config.getUsername(),
                        StringUtils.isEmpty(config.getPassword())
                                ? StringUtils.EMPTY
                                : config.getPassword(),
                        clientBuilder,
                        format);
        String version = influxdb.version();
        if (!influxdb.ping().isGood()) {
            throw new InfluxdbConnectorException(
                    InfluxdbConnectorErrorCode.CONNECT_FAILED,
                    String.format("Connect influxdb failed, the url is: {%s}", config.getUrl()));
        }
        log.info("connect influxdb successful. sever version :{}.", version);
        return influxdb;
    }

    public static void setWriteProperty(InfluxDB influxdb, SinkConfig sinkConfig) {
        String rp = sinkConfig.getRp();
        if (!StringUtils.isEmpty(rp)) {
            influxdb.setRetentionPolicy(rp);
        }
    }

    public static InfluxDB getWriteClient(SinkConfig sinkConfig) throws ConnectException {
        InfluxDB influxdb = getInfluxDB(sinkConfig);
        influxdb.setDatabase(sinkConfig.getDatabase());
        setWriteProperty(getInfluxDB(sinkConfig), sinkConfig);
        return influxdb;
    }
}
