package org.axonframework.cassandra.eventsourcing.integrationtests.config;

import com.datastax.driver.core.Cluster;
import org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(CassandraConfig.class)
public class AxonTestContext {

	@Bean
	public Serializer serializer(){
		return new XStreamSerializer();
	}

	@Bean
	public EventStorageEngine eventStore(Cluster cluster, Serializer serializer){
		return new CassandraEventStorageEngine(cluster, serializer);
	}
}
