package org.apache.pulsar.common.tls;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.netty.handler.ssl.SslProvider;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.cert.X509Certificate;
import java.util.Set;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.Data;
import org.apache.pulsar.common.util.SecurityUtility;

@Data
@Builder
public class SslContextStoreConf {
    private Supplier<Iterable<X509Certificate>> trustCertSupplier;
    private Supplier<PrivateKey> keySupplier;
    private Supplier<Iterable<X509Certificate>> certSupplier;

    private SslProvider sslProvider;
    private Provider sslContextProvider;

    private Set<String> ciphers;
    private Set<String> protocols ;

    private boolean forClient;

    private int refreshIntervalSeconds;

    private boolean requireTrustedClientCertOnConnect;
    private boolean allowInsecureConnection;

    public Provider getSslContextProvider() {
        if (sslContextProvider == null) {
            return SecurityUtility.CONSCRYPT_PROVIDER;
        }
        return sslContextProvider;
    }

    public Iterable<String> getCiphers() {
        if (ciphers == null || Iterables.isEmpty(ciphers)) {
            return Sets.newHashSet("TLSv1.2", "TLSv1.3");
        }

        return ciphers;
    }
}
