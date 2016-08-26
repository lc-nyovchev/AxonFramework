package org.axonframework.cassandra.eventsourcing.eventstore;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

public class CassandraEventStorageStrategy {

	public static final String AGGREGATE_IDENTIFIER = "aggregate_identifier";
	public static final String TIMESTAMP = "timestamp";
	public static final String PAYLOAD_TYPE = "payload_type";
	public static final String PAYLOAD = "payload";
	public static final String EVENT_IDENTIFIER = "event_identifier";

	private static final String KEYSPACE_NAME = "axon_events";
	private static final String DOMAIN_EVENTS_CF_NAME = "domain_event_entry";
	private static final String SNAPSHOT_EVENTS_CF_NAME = "snapshot_domain_event_entry";
	private static final String SAGA_EVENTS_CF_NAME = "saga_entry";

	private final Session session;
	private final Serializer serializer;

	public CassandraEventStorageStrategy(Cluster cluster, Serializer serializer) {
		this.session = cluster.newSession();
		this.serializer = serializer;
	}

	public void appendEvents(List<? extends EventMessage<?>> events) {
		BatchStatement statement = new BatchStatement();
		events.forEach(event -> {
			DomainEventMessage genericDomainEventMessage = (DomainEventMessage) event;
			statement.add(QueryBuilder.insertInto(KEYSPACE_NAME, DOMAIN_EVENTS_CF_NAME)
					.value(AGGREGATE_IDENTIFIER, UUID.fromString(genericDomainEventMessage.getAggregateIdentifier()))
					.value(TIMESTAMP, genericDomainEventMessage.getTimestamp().toEpochMilli())
					.value(PAYLOAD_TYPE, genericDomainEventMessage.getPayloadType().getName())
					.value(PAYLOAD, getBytes(genericDomainEventMessage))
					//.value(EVENT_IDENTIFIER, genericDomainEventMessage.getIdentifier()) //TODO
					.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));
		});
		session.execute(statement);

	}

	public DomainEventStream readEvents(String aggregateIdentifier, long firstSequenceNumber) {
		ResultSet resultSet = session.execute(QueryBuilder.select().from(KEYSPACE_NAME, DOMAIN_EVENTS_CF_NAME)
				.where(QueryBuilder.eq(AGGREGATE_IDENTIFIER, UUID.fromString(aggregateIdentifier)))
				.and(QueryBuilder.gte(TIMESTAMP, firstSequenceNumber))
				.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));
		return convert(resultSet);

	}

	public Stream<? extends TrackedEventMessage<?>> readEvents(TrackingToken trackingToken, boolean mayBlock) {
		if (!(trackingToken instanceof CassandraTrackingToken)){
			throw new IllegalArgumentException("Supplied token wasn't a CassandraTrackingToken");
		}
		CassandraTrackingToken cassandraTrackingToken = (CassandraTrackingToken) trackingToken;
		ResultSet resultSet = session.execute(QueryBuilder.select().from(KEYSPACE_NAME, DOMAIN_EVENTS_CF_NAME)
				.where(QueryBuilder.eq(AGGREGATE_IDENTIFIER, UUID.fromString(cassandraTrackingToken.getAggregateIdentifier())))
				.and(QueryBuilder.gte(TIMESTAMP, cassandraTrackingToken.getTimestamp()))
				.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));
		return convert(resultSet).asStream()
				.map(CassandraTrackedEventMessage::new);
	}

	public void storeSnapshot(DomainEventMessage<?> snapshot) {
			QueryBuilder.insertInto(KEYSPACE_NAME, DOMAIN_EVENTS_CF_NAME)
					.value(AGGREGATE_IDENTIFIER, UUID.fromString(snapshot.getAggregateIdentifier()))
					.value(TIMESTAMP, snapshot.getTimestamp().toEpochMilli())
					.value(PAYLOAD_TYPE, snapshot.getPayloadType().getName())
					.value(PAYLOAD, getBytes(snapshot))
					//.value(EVENT_IDENTIFIER, genericDomainEventMessage.getIdentifier()) //TODO
					.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
	}

	public Optional<DomainEventMessage<?>> readSnapshot(String aggregateIdentifier) {
		return null;
	}

	private ByteBuffer getBytes(EventMessage<?> event){
		//TODO revisit this mechanism of wrapping it to String all the time
		return ByteBuffer.wrap(serializer.serialize(event.getPayload(), String.class)
			.getData().getBytes());
	}


	private DomainEventStream convert(ResultSet resultSet) {
		return new CassandraEventStream(serializer, resultSet);
	}

}
