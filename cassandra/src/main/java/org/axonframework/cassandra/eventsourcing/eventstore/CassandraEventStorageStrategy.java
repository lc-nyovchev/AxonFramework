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
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

/**
 * A basic opinionated storage strategy for Cassandra that uses predefined keyspaces and column family names and
 * requires aggregate identifiers to be UUIDs (which is the only sensible thing for an aggregate, anyway)
 *
 * @author Nikola Yovchev
 */
public class CassandraEventStorageStrategy {

	public static final String AGGREGATE_IDENTIFIER = "aggregate_identifier";
	public static final String TIMESTAMP = "timestamp";
	public static final String PAYLOAD_TYPE = "payload_type";
	public static final String PAYLOAD = "payload";
	public static final String EVENT_IDENTIFIER = "event_identifier";
	public static final String SNAPSHOT_IDENTIFIER = "snapshot_identifier";
	public static final String METADATA = "metadata";

	private static final String KEYSPACE_NAME = "axon_events";
	private static final String DOMAIN_EVENTS_CF_NAME = "domain_event_entry";
	private static final String SNAPSHOT_EVENTS_CF_NAME = "snapshot_domain_event_entry";

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
				.value(PAYLOAD, getBytesForObject(genericDomainEventMessage.getPayload()))
				.value(EVENT_IDENTIFIER, genericDomainEventMessage.getIdentifier())
				.value(METADATA, toStringMap(genericDomainEventMessage.getMetaData()))
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
		QueryBuilder.insertInto(KEYSPACE_NAME, SNAPSHOT_EVENTS_CF_NAME)
			.value(AGGREGATE_IDENTIFIER, UUID.fromString(snapshot.getAggregateIdentifier()))
			.value(TIMESTAMP, snapshot.getTimestamp().toEpochMilli())
			.value(PAYLOAD_TYPE, snapshot.getPayloadType().getName())
			.value(PAYLOAD, getBytesForObject(snapshot.getPayload()))
			.value(SNAPSHOT_IDENTIFIER, snapshot.getIdentifier())
			.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
	}

	public Optional<DomainEventMessage<?>> readSnapshot(String aggregateIdentifier) {
		ResultSet resultSet = session.execute(QueryBuilder.select().from(KEYSPACE_NAME, DOMAIN_EVENTS_CF_NAME)
			.where(QueryBuilder.eq(AGGREGATE_IDENTIFIER, UUID.fromString(aggregateIdentifier)))
			.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));
		DomainEventMessage<?> domainEventMessage = convert(resultSet)
			.asStream().findFirst().get();
		return Optional.ofNullable(domainEventMessage);
	}


	private Map<String, String> toStringMap(MetaData metaData) {
		return metaData.entrySet().stream()
			.collect(toMap(Map.Entry::getKey, metadataValue ->
				serializer.serialize(metadataValue.getValue(), String.class).getData()
			));
	}

	private ByteBuffer getBytesForObject(Object value){
		return ByteBuffer.wrap(serializer.serialize(value, String.class)
			.getData().getBytes());
	}


	private DomainEventStream convert(ResultSet resultSet) {
		return new CassandraEventStream(serializer, resultSet);
	}

}
