package org.axonframework.cassandra.eventsourcing.eventstore;


import com.datastax.driver.core.Cluster;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.serialization.Serializer;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * A Storage engine that stores data in a Cassandra Event Store using a {@link CassandraEventStorageStrategy}
 * @see CassandraEventStorageStrategy
 * @author Nikola Yovchev
 */
public class CassandraEventStorageEngine implements EventStorageEngine {

	private CassandraEventStorageStrategy cassandraEventStorageStrategy;

	public CassandraEventStorageEngine(Cluster cluster, Serializer serializer){
		this.cassandraEventStorageStrategy = new CassandraEventStorageStrategy(cluster, serializer);
	}

	@Override
	public void appendEvents(List<? extends EventMessage<?>> events) {
		this.cassandraEventStorageStrategy.appendEvents(events);
	}

	@Override
	public void storeSnapshot(DomainEventMessage<?> snapshot) {
		this.cassandraEventStorageStrategy.storeSnapshot(snapshot);
	}

	@Override
	public Stream<? extends TrackedEventMessage<?>> readEvents(TrackingToken trackingToken, boolean mayBlock) {
		return this.cassandraEventStorageStrategy.readEvents(trackingToken, mayBlock);
	}

	@Override
	public DomainEventStream readEvents(String aggregateIdentifier, long firstSequenceNumber) {
		return this.cassandraEventStorageStrategy.readEvents(aggregateIdentifier, firstSequenceNumber);
	}

	@Override
	public Optional<DomainEventMessage<?>> readSnapshot(String aggregateIdentifier) {
		return this.cassandraEventStorageStrategy.readSnapshot(aggregateIdentifier);
	}
}
