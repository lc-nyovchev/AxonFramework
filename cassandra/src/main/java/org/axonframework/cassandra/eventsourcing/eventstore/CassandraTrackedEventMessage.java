package org.axonframework.cassandra.eventsourcing.eventstore;

import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.eventsourcing.eventstore.TrackingToken;

public class CassandraTrackedEventMessage<T> extends GenericDomainEventMessage<T> implements TrackedEventMessage<T> {

	private static final long serialVersionUID = 1863887388011622937L;

	private final DomainEventMessage<?> domainEventMessage;

	public CassandraTrackedEventMessage(DomainEventMessage<? extends T> domainEventMessage) {
		super(domainEventMessage.getType(), domainEventMessage.getAggregateIdentifier(), domainEventMessage.getSequenceNumber(), domainEventMessage.getPayload());
		this.domainEventMessage = domainEventMessage;
	}

	@Override
	public TrackingToken trackingToken() {
		return new CassandraTrackingToken(domainEventMessage.getAggregateIdentifier(), domainEventMessage.getTimestamp().toEpochMilli());
	}

}
