package convex.db.psql;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.bouncycastle.util.io.pem.PemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

/**
 * Generates and loads the internal CA + per-node TLS certificate that make
 * PG-wire TLS default-on (see {@code dbase}'s {@code PLAN_access_rights.md},
 * "TLS on PG-wire"). One small self-managed CA signs every node's own leaf
 * certificate, rather than each node minting an independent self-signed
 * cert — a client (psql's {@code ~/.postgresql/root.crt}, or a mobile app's
 * bundled pinned cert) trusts the one CA root once, and every node
 * validates against it with no client-side update needed as the fleet
 * grows.
 *
 * <p>The CA key pair is deliberately file-based, not replicated — sharing
 * it across hosts in a fleet is an operator's deployment step (copy the CA
 * cert/key pair to each host before that host's first node starts), the
 * same trust model this project already uses for {@code NodeIdentity}'s
 * own key file. A host with no CA files present generates a fresh CA on
 * first use, exactly like a node with no {@code NodeIdentity} key does —
 * each independently-bootstrapped host then has its own CA until an
 * operator deliberately unifies them by copying one over the other.
 *
 * <p>Not yet built: certificate rotation. Both the CA (10 year validity)
 * and each leaf cert (5 years) are generated once and reused indefinitely;
 * an operator wanting rotation has to delete the files and let this class
 * regenerate them, which invalidates every existing client's pinned trust
 * in the old CA — a real operational event, not yet automated.
 */
public final class TlsSupport {

	private static final Logger log = LoggerFactory.getLogger(TlsSupport.class);

	static {
		if (Security.getProvider("BC") == null) {
			Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
		}
	}

	private TlsSupport() {}

	/**
	 * Builds (generating and persisting on first use) the CA and this
	 * node's own leaf certificate, and returns a server {@link SslContext}
	 * ready to hand to {@code PgServer}.
	 *
	 * @param caCertFile  the CA's own certificate (PEM) — shared across a
	 *                    fleet by an operator copying this file, not by
	 *                    replication
	 * @param caKeyFile   the CA's own private key (PEM) — copied alongside
	 *                    {@code caCertFile} to sign further nodes' certs;
	 *                    not needed once a node's own leaf cert already
	 *                    exists, only to mint one
	 * @param nodeCertFile this node's own leaf certificate (PEM)
	 * @param nodeKeyFile  this node's own leaf private key (PEM)
	 * @param hostname     the hostname to bind the leaf cert's CN/SAN to
	 */
	public static SslContext buildServerSslContext(File caCertFile, File caKeyFile,
			File nodeCertFile, File nodeKeyFile, String hostname) throws Exception {
		CertAndKey ca = loadOrGenerateCa(caCertFile, caKeyFile);
		CertAndKey node = loadOrGenerateLeaf(nodeCertFile, nodeKeyFile, hostname, ca);
		return SslContextBuilder.forServer(node.key, node.cert, ca.cert).build();
	}

	private record CertAndKey(X509Certificate cert, PrivateKey key) {}

	private static CertAndKey loadOrGenerateCa(File certFile, File keyFile) throws Exception {
		if (certFile.exists() && keyFile.exists()) {
			log.info("Loading existing TLS CA from {}", certFile);
			return new CertAndKey(readCert(certFile), readKey(keyFile));
		}

		log.info("Generating new TLS CA at {} (first use on this host)", certFile);
		KeyPair caKeyPair = generateKeyPair();
		X500Name subject = new X500Name("CN=dbase internal CA");
		BigInteger serial = randomSerial();
		Instant notBefore = Instant.now();
		Instant notAfter = notBefore.plus(Duration.ofDays(3650)); // 10 years

		JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
			subject, serial, Date.from(notBefore), Date.from(notAfter), subject, caKeyPair.getPublic());
		JcaX509ExtensionUtils extUtils = new JcaX509ExtensionUtils();
		builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(0));
		builder.addExtension(Extension.keyUsage, true,
			new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
		builder.addExtension(Extension.subjectKeyIdentifier, false,
			extUtils.createSubjectKeyIdentifier(caKeyPair.getPublic()));

		X509Certificate caCert = sign(builder, caKeyPair.getPrivate());
		writeCert(certFile, caCert);
		writeKey(keyFile, caKeyPair.getPrivate());
		return new CertAndKey(caCert, caKeyPair.getPrivate());
	}

	private static CertAndKey loadOrGenerateLeaf(File certFile, File keyFile, String hostname, CertAndKey ca)
			throws Exception {
		if (certFile.exists() && keyFile.exists()) {
			log.info("Loading existing TLS certificate from {}", certFile);
			return new CertAndKey(readCert(certFile), readKey(keyFile));
		}

		log.info("Generating new TLS certificate for '{}' at {}, signed by {}", hostname, certFile, ca.cert.getSubjectX500Principal());
		KeyPair leafKeyPair = generateKeyPair();
		X500Name issuer = new X500Name(ca.cert.getSubjectX500Principal().getName());
		X500Name subject = new X500Name("CN=" + hostname);
		BigInteger serial = randomSerial();
		Instant notBefore = Instant.now();
		Instant notAfter = notBefore.plus(Duration.ofDays(1825)); // 5 years

		JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
			issuer, serial, Date.from(notBefore), Date.from(notAfter), subject, leafKeyPair.getPublic());
		JcaX509ExtensionUtils extUtils = new JcaX509ExtensionUtils();
		builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
		builder.addExtension(Extension.keyUsage, true,
			new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
		builder.addExtension(Extension.extendedKeyUsage, false,
			new ExtendedKeyUsage(KeyPurposeId.id_kp_serverAuth));
		builder.addExtension(Extension.subjectKeyIdentifier, false,
			extUtils.createSubjectKeyIdentifier(leafKeyPair.getPublic()));
		builder.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(new GeneralName[] {
			new GeneralName(GeneralName.dNSName, hostname),
			new GeneralName(GeneralName.dNSName, "localhost"),
			new GeneralName(GeneralName.iPAddress, "127.0.0.1"),
		}));

		X509Certificate leafCert = sign(builder, ca.key);
		writeCert(certFile, leafCert);
		writeKey(keyFile, leafKeyPair.getPrivate());
		return new CertAndKey(leafCert, leafKeyPair.getPrivate());
	}

	private static KeyPair generateKeyPair() throws Exception {
		KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
		gen.initialize(2048, new SecureRandom());
		return gen.generateKeyPair();
	}

	private static BigInteger randomSerial() {
		return new BigInteger(128, new SecureRandom());
	}

	private static X509Certificate sign(JcaX509v3CertificateBuilder builder, PrivateKey signerKey) throws Exception {
		ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(signerKey);
		X509CertificateHolder holder = builder.build(signer);
		return new JcaX509CertificateConverter().getCertificate(holder);
	}

	private static X509Certificate readCert(File file) throws Exception {
		try (PemReader reader = new PemReader(new FileReader(file))) {
			PemObject obj = reader.readPemObject();
			CertificateFactory cf = CertificateFactory.getInstance("X.509");
			return (X509Certificate) cf.generateCertificate(new java.io.ByteArrayInputStream(obj.getContent()));
		}
	}

	private static PrivateKey readKey(File file) throws Exception {
		try (PemReader reader = new PemReader(new FileReader(file))) {
			PemObject obj = reader.readPemObject();
			KeyFactory kf = KeyFactory.getInstance("RSA");
			return kf.generatePrivate(new PKCS8EncodedKeySpec(obj.getContent()));
		}
	}

	private static void writeCert(File file, X509Certificate cert) throws Exception {
		ensureParentDir(file);
		try (PemWriter writer = new PemWriter(new FileWriter(file))) {
			writer.writeObject(new PemObject("CERTIFICATE", cert.getEncoded()));
		}
	}

	private static void writeKey(File file, PrivateKey key) throws Exception {
		ensureParentDir(file);
		try (PemWriter writer = new PemWriter(new FileWriter(file))) {
			writer.writeObject(new PemObject("PRIVATE KEY", key.getEncoded()));
		}
	}

	private static void ensureParentDir(File file) throws IOException {
		File parent = file.getParentFile();
		if (parent != null && !parent.exists() && !parent.mkdirs()) {
			throw new IOException("Could not create directory: " + parent);
		}
	}
}
