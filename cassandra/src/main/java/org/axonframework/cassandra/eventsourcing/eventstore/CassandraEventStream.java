package org.axonframework.cassandra.eventsourcing.eventstore;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.SerializedType;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;
import static org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageStrategy.AGGREGATE_IDENTIFIER;
import static org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageStrategy.EVENT_IDENTIFIER;
import static org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageStrategy.METADATA;
import static org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageStrategy.PAYLOAD;
import static org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageStrategy.PAYLOAD_TYPE;
import static org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageStrategy.TIMESTAMP;

/**
 * A stream of Cassandra events
 * @author Nikola Yovchev
 */
public class CassandraEventStream implements DomainEventStream {

	private final ResultSet resultSet;
	private final Serializer serializer;
	private final PeekingIterator<Row> peekingIterator;

	public CassandraEventStream(Serializer serializer, ResultSet resultSet){
		this.resultSet = resultSet;
		this.serializer = serializer;
		this.peekingIterator = Iterators.peekingIterator(this.resultSet.iterator());
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
	public DomainEventMessage<?> peek(){
		Row one = peekingIterator.peek();
		if (one != null){
			return toTrackedEvent(one);
		} else {
			return null;
		}
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
					getMetadata(one),
					one.getString(EVENT_IDENTIFIER),
					Instant.ofEpochMilli(timestampAndSequenceNr)
			);
		} catch (Exception ex){
			throw new RuntimeException(ex);
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

	private MetaData getMetadata(Row one) {
		Map<String, String> metadata = one.getMap(METADATA, String.class, String.class);
		if (metadata != null){
			Map<String, Object> converted = metadata.entrySet().stream()
				.collect(toMap(Map.Entry::getKey, stringStringEntry -> {
						SimpleSerializedObject<String> stringSimpleSerializedObject = new SimpleSerializedObject<>(
								stringStringEntry.getValue(),
								String.class,
								serializer.typeForClass(Object.class)
						);
						return serializer.deserialize(stringSimpleSerializedObject);
					}
				));
			return MetaData.from(converted);

		}
		return MetaData.emptyInstance();
	}
}
