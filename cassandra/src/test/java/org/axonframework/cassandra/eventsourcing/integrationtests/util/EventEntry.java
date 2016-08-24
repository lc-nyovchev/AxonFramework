package org.axonframework.cassandra.eventsourcing.integrationtests.util;


import lombok.Value;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.messaging.MetaData;

import java.time.Instant;
import java.util.Map;

@Value
public class EventEntry<T> implements EventMessage<T> {

	private static final long serialVersionUID = 5140768527895326384L;

	private String identifier;
	private Instant timestamp;
	private T payload;
	private Class<T> payloadType;
	private MetaData metaData = MetaData.emptyInstance();


	@Override
	public EventMessage<T> withMetaData(Map<String, ?> metaData) {
		return this;
	}

	@Override
	public EventMessage<T> andMetaData(Map<String, ?> metaData) {
		return this;
	}
}
