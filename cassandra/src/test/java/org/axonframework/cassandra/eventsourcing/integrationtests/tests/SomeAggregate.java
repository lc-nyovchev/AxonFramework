package org.axonframework.cassandra.eventsourcing.integrationtests.tests;


import lombok.NoArgsConstructor;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.events.SomeAggregateCreatedEvent;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.events.SomeAggregateNameChangedEvent;
import org.axonframework.commandhandling.model.AggregateIdentifier;
import org.axonframework.commandhandling.model.AggregateLifecycle;
import org.axonframework.eventsourcing.EventSourcingHandler;

import java.util.UUID;

@NoArgsConstructor
public class SomeAggregate {

	@AggregateIdentifier private UUID someAggregateId;
	private String name;

	public SomeAggregate(UUID someAggregateId){
		this.someAggregateId = someAggregateId;
		AggregateLifecycle.apply(new SomeAggregateCreatedEvent(this.someAggregateId));
	}

	public void setName(String name){
		this.name = name;
		AggregateLifecycle.apply(new SomeAggregateNameChangedEvent(this.someAggregateId, this.name));
	}

	@EventSourcingHandler
	public void on(SomeAggregateCreatedEvent createdEvent){
		this.someAggregateId = createdEvent.getSomeAggregateId();
	}

	@EventSourcingHandler
	public void on(SomeAggregateNameChangedEvent someAggregateNameChangedEvent){
		this.name = someAggregateNameChangedEvent.getName();
	}
}
