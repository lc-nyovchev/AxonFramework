package org.axonframework.cassandra.eventsourcing.eventstore;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.SerializedType;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;

import java.time.Instant;
import java.util.UUID;

import static org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageStrategy.AGGREGATE_IDENTIFIER;
import static org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageStrategy.PAYLOAD;
import static org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageStrategy.PAYLOAD_TYPE;
import static org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageStrategy.TIMESTAMP;

public class CassandraEventStream implements DomainEventStream {

	private final ResultSet resultSet;
	private final Serializer serializer;

	public CassandraEventStream(Serializer serializer, ResultSet resultSet){
		this.resultSet = resultSet;
		this.serializer = serializer;
	}

	@Override
	public boolean hasNext() {
		return !resultSet.isExhausted();
	}

	@Override
	public DomainEventMessage next() {
		if (resultSet.isExhausted()){
			return null;
		}
		Row one = resultSet.one();
		return toTrackedEvent(one);
	}

	@Override
	public DomainEventMessage<?> peek() {
		throw new UnsupportedOperationException("Peeking is not supported");
	}

	private DomainEventMessage toTrackedEvent(Row one) {
		UUID aggregateId = one.getUUID(AGGREGATE_IDENTIFIER);
		Long timestampAndSequenceNr = one.getLong(TIMESTAMP);
		String type = one.getString(PAYLOAD_TYPE);
		try {
			Object deserializedObject = getDeserializedObject(one, type);
			return new GenericDomainEventMessage(
				type,
				aggregateId.toString(),
				timestampAndSequenceNr,
				deserializedObject,
				MetaData.emptyInstance(),
				UUID.randomUUID().toString(),
				Instant.ofEpochMilli(timestampAndSequenceNr)
			);
		} catch (Exception e) {
			return null;
		}
	}

	private Object getDeserializedObject(Row one, String type) throws ClassNotFoundException {
		Class<?> classType = Class.forName(type);
		SerializedType serializedType = serializer.typeForClass(classType);
		//TODO revisit this mechanism of wrapping it to String all the time
		SimpleSerializedObject<String> stringSimpleSerializedObject = new SimpleSerializedObject<>(
			new String(one.getBytes(PAYLOAD).array()),
			String.class,
			serializedType
		);
		return serializer.deserialize(stringSimpleSerializedObject);
	}
}
