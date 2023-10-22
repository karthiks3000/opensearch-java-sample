package samples;


import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.opensearch.client.RestClient;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.opensearch.client.transport.rest_client.RestClientTransport;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

public class SampleClient {
    public static OpenSearchClient create() throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        var env = System.getenv();
        var https = Boolean.parseBoolean(env.getOrDefault("HTTPS", "true"));
        var hostname = env.getOrDefault("HOST", "localhost");
        var port = Integer.parseInt(env.getOrDefault("PORT", "9200"));
        var user = env.getOrDefault("USERNAME", "admin");
        var pass = env.getOrDefault("PASSWORD", "admin");

        final var hosts = new HttpHost[] { new HttpHost(https ? "https" : "http", hostname, port) };

        final var sslContext = SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build();

        final var transport = ApacheHttpClient5TransportBuilder.builder(hosts)
                .setMapper(new JacksonJsonpMapper())
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    final var credentialsProvider = new BasicCredentialsProvider();
                    for (final var host : hosts) {
                        credentialsProvider.setCredentials(new AuthScope(host), new UsernamePasswordCredentials(user, pass.toCharArray()));
                    }

                    // Disable SSL/TLS verification as our local testing clusters use self-signed certificates
                    final var tlsStrategy = ClientTlsStrategyBuilder.create()
                            .setSslContext(sslContext)
                            .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                            .build();

                    final var connectionManager = PoolingAsyncClientConnectionManagerBuilder.create().setTlsStrategy(tlsStrategy).build();

                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setConnectionManager(connectionManager);
                })
                .build();
        return new OpenSearchClient(transport);
    }

    public static OpenSearchClient createRestClient()  {
        var env = System.getenv();
        var https = Boolean.parseBoolean(env.getOrDefault("HTTPS", "true"));
        var hostname = env.getOrDefault("HOST", "localhost");
        var port = Integer.parseInt(env.getOrDefault("PORT", "9200"));
        var user = env.getOrDefault("USERNAME", "admin");
        var pass = env.getOrDefault("PASSWORD", "admin");
        final org.apache.http.HttpHost[] restHosts = new org.apache.http.HttpHost[] {new org.apache.http.HttpHost(hostname, port, https ? "https" : "http")};


        RestClient restClient = RestClient.builder(restHosts)
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    final org.apache.http.impl.client.BasicCredentialsProvider credentialsProvider = new org.apache.http.impl.client.BasicCredentialsProvider();
                    for (final org.apache.http.HttpHost ignored : restHosts) {
                        credentialsProvider.setCredentials(org.apache.http.auth.AuthScope.ANY, new org.apache.http.auth.UsernamePasswordCredentials(user, pass));
                    }

                    try {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                                // disable the certificate since our testing cluster just uses the default security configuration
                                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                                .setSSLContext(SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).build();

        final RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new OpenSearchClient(transport);
    }
}
