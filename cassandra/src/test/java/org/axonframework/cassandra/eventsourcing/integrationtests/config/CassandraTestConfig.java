package org.axonframework.cassandra.eventsourcing.integrationtests.config;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import org.axonframework.cassandra.eventsourcing.integrationtests.cassandra.EmbeddedCassandra;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import java.io.IOException;

/**
 * @author Nikola Yovchev
 */
@Configuration
public class CassandraTestConfig {

	private static final String EMBEDDED_CASSANDRA_BEAN_NAME = "EmbeddedCassandra";

	private EmbeddedCassandra embeddedCassandra;

	@Bean(destroyMethod = "stop", name = EMBEDDED_CASSANDRA_BEAN_NAME)
	public EmbeddedCassandra embeddedCassandra() throws IOException {
		this.embeddedCassandra = new EmbeddedCassandra();
		this.embeddedCassandra.start();
		return this.embeddedCassandra;
	}

	@Bean
	@DependsOn(value = EMBEDDED_CASSANDRA_BEAN_NAME)
	public Cluster getCluster(){
		Cluster cluster = Cluster.builder()
				.addContactPoint("localhost")
				.withProtocolVersion(ProtocolVersion.V3) //cass 3+ supports 4
				.withLoadBalancingPolicy(
						DCAwareRoundRobinPolicy.builder()
								.withLocalDc("DC1")
								.build()
				)
				.build();
		cluster.getConfiguration()
				.getProtocolOptions()
				.setCompression(ProtocolOptions.Compression.LZ4);
		return cluster;
	}
}
