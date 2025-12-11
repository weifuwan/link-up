package org.apache.cockpit.connectors.doris.util;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.RequestContent;

/** util to build http client. */
public class HttpUtil {
    private final HttpClientBuilder httpClientBuilder =
            HttpClients.custom()
                    .setRedirectStrategy(
                            new DefaultRedirectStrategy() {
                                @Override
                                protected boolean isRedirectable(String method) {
                                    return true;
                                }
                            })
                    .addInterceptorLast(new RequestContent(true));;

    public CloseableHttpClient getHttpClient() {
        return httpClientBuilder.build();
    }
}
