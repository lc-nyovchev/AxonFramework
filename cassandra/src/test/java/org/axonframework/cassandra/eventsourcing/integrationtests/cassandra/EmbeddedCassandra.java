package org.axonframework.cassandra.eventsourcing.integrationtests.cassandra;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.commons.io.IOUtils;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * An embedded cassandra starter that looks in the resource directory for the definition of keyspaces and column families and
 * creates them upon startup. Upon dying, the deamon clears up the created keyspaces/column families
 *
 * @author Nikola Yovchev
 */
@Slf4j
public class EmbeddedCassandra {

	private CassandraDaemon cassandraDaemon;
	private Cluster cluster;
	private EmbeddedCassandraCleaner embeddedCassandraCleaner;

	public EmbeddedCassandra() throws IOException {

		this.embeddedCassandraCleaner = new EmbeddedCassandraCleaner();
		this.embeddedCassandraCleaner.prepare();
		this.cassandraDaemon = new CassandraDaemon();
		this.cassandraDaemon.init(null);
		this.cassandraDaemon.start();
		waitForServerToStart();
	}

	private void waitForServerToStart() {
		try {
			int numAttempts = 0;
			while (!this.cassandraDaemon.nativeServer.isRunning()) {
				if (numAttempts > 10) {
					throw new RuntimeException("Cassandra failed to start within the allocated time");
				}
				numAttempts++;
				Thread.sleep(500L);
			}
		} catch (InterruptedException ex) {
			throw new RuntimeException(ex);
		}

	}

	public void start() throws IOException {
		log.debug("Starting cassandra daemon");
		cluster = Cluster.builder()
				.addContactPointsWithPorts(ImmutableSet.of(new InetSocketAddress("localhost", 9042)))
				.build();
		createKeyspacesAndColumnFamilies();
	}

	public void stop() throws IOException {
		log.debug("Stopping cassandra daemon");
		cassandraDaemon.stop();
		embeddedCassandraCleaner.cleanupDataDirectories();
	}

	private void createKeyspacesAndColumnFamilies() throws IOException {
		try (
				Session session = cluster.connect();
				InputStream inputStream = new ClassPathResource("cassandra-init.def").getInputStream()
		) {
			IOUtils.readLines(inputStream, StandardCharsets.UTF_8).forEach(
					line -> {
						log.debug("Executing: '{}'", line);
						session.execute(line);
					}
			);
		}
	}
}