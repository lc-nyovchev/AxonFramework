package org.axonframework.cassandra.eventsourcing.eventstore;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.thoughtworks.xstream.XStream;
import org.axonframework.common.io.IOUtils;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.UUID;

import static org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageStrategy.AGGREGATE_IDENTIFIER;
import static org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageStrategy.PAYLOAD;
import static org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageStrategy.PAYLOAD_TYPE;
import static org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageStrategy.TIMESTAMP;

public class CassandraEventStream implements DomainEventStream {

	private final ResultSet resultSet;
	private final Serializer serializer;
	private final XStream xStream;

	public CassandraEventStream(Serializer serializer, ResultSet resultSet){
		this.resultSet = resultSet;
		this.serializer = serializer;
		this.xStream = new XStream();
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
		ByteBuffer payload = one.getBytes(PAYLOAD);
		Object o = xStream.fromXML(new ByteArrayInputStream(payload.array()));
		return new GenericDomainEventMessage(type, aggregateId.toString(), timestampAndSequenceNr, o, MetaData.emptyInstance(), UUID.randomUUID().toString(), Instant.ofEpochMilli(timestampAndSequenceNr));
	}
}
