package org.apache.pulsar.common.tls;

import com.google.common.collect.Iterables;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;
import java.util.Objects;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import org.apache.pulsar.common.util.KeyStoreHolder;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.common.util.SslContextAutoRefreshBuilder;

public class SslContextStore extends SslContextAutoRefreshBuilder<SslContext> {
    private volatile Iterable<X509Certificate> trustCert;
    private volatile PrivateKey key;
    private volatile Iterable<X509Certificate> cert;

    private volatile SslContext sslContext;

    private final SslContextStoreConf conf;

    public SslContext newSslContext()
            throws SSLException, KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException {
        trustCert = conf.getTrustCertSupplier().get();
        key = conf.getKeySupplier().get();
        cert = conf.getCertSupplier().get();

        KeyManager[] keyManagers =
                SecurityUtility.setupKeyManager(new KeyStoreHolder(), key,
                        Iterables.toArray(cert, X509Certificate.class));
        if (keyManagers == null || keyManagers.length > 1) {
            throw new KeyStoreException("keyManagers size should be 1");
        }

        TrustManager[] trustManagers =
                SecurityUtility.setupTrustCerts(new KeyStoreHolder(), conf.isAllowInsecureConnection(),
                        Iterables.toArray(trustCert, X509Certificate.class), conf.getSslContextProvider());
        if (trustManagers != null && trustManagers.length > 1) {
            throw new KeyStoreException("TrustManager size should be 1");
        }

        SslContextBuilder sslContextBuilder;
        if (conf.isForClient()) {
            sslContextBuilder = SslContextBuilder.forClient();
        } else {
            sslContextBuilder = SslContextBuilder.forServer(keyManagers[0])
                    .clientAuth(conf.isRequireTrustedClientCertOnConnect() ? ClientAuth.REQUIRE : ClientAuth.OPTIONAL);
        }
        sslContextBuilder.sslProvider(conf.getSslProvider())
                .sslContextProvider(conf.getSslContextProvider())
                .ciphers(conf.getCiphers())
                .protocols(conf.getProtocols());
        if (trustManagers != null) {
            sslContextBuilder.trustManager(trustManagers[0]);
        }
        return sslContextBuilder.build();
    }

    public SslContextStore(SslContextStoreConf conf) {
        super(conf.getRefreshIntervalSeconds());
        this.conf = conf;
    }

    @Override
    protected SslContext update() throws GeneralSecurityException, IOException {
        sslContext = newSslContext();
        return sslContext;
    }

    @Override
    protected SslContext getSslContext() {
        return this.sslContext;
    }

    @Override
    protected boolean needUpdate() {
        if (conf.getRefreshIntervalSeconds() < 0) {
            return false;
        }

        if (!Objects.equals(conf.getKeySupplier().get(), key)) {
            return true;
        }

        if (iterableNotEquals(conf.getCertSupplier().get(), cert)) {
            return true;
        }

        return iterableNotEquals(conf.getTrustCertSupplier().get(), trustCert);
    }

    private boolean iterableNotEquals(Iterable<?> iterable1, Iterable<?> iterable2) {
        if (iterable1 == iterable2) {
            return false;
        }

        if (iterable1 == null || iterable2 == null) {
            return true;
        }

        return !Iterables.elementsEqual(iterable1, iterable2);
    }
}
