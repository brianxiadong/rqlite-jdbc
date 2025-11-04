package io.rqlite.client;

import javax.net.ssl.*;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class L4Http {

  // Configure global HTTPS to trust all certificates (insecure)
  public static void configureInsecureTLS() throws Exception {
    SSLContext sslContext = SSLContext.getInstance("TLS");
    TrustManager[] trustAll = new TrustManager[]{
      new X509TrustManager() {
        public void checkClientTrusted(X509Certificate[] chain, String authType) {}
        public void checkServerTrusted(X509Certificate[] chain, String authType) {}
        public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
      }
    };
    sslContext.init(null, trustAll, new SecureRandom());
    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
    HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
      @Override public boolean verify(String hostname, SSLSession session) { return true; }
    });
  }

  // Configure global HTTPS to trust the provided CA certificate
  public static void configureTLSWithCACert(String caCertPath) throws Exception {
    CertificateFactory cf = CertificateFactory.getInstance("X.509");
    byte[] caBytes;
    try (FileInputStream fis = new FileInputStream(caCertPath)) {
      caBytes = new byte[fis.available()];
      int read = fis.read(caBytes);
      if (read <= 0) {
        throw new IllegalStateException("Failed to read CA certificate: " + caCertPath);
      }
    }
    ByteArrayInputStream bis = new ByteArrayInputStream(caBytes);
    X509Certificate caCert = (X509Certificate) cf.generateCertificate(bis);

    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(null, null);
    ks.setCertificateEntry("caCert", caCert);

    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ks);

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, tmf.getTrustManagers(), new SecureRandom());

    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
    // Use default hostname verifier; do not override
  }
}
