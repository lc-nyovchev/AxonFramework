package org.axonframework.cassandra.eventsourcing.integrationtests.tests.commands;


import lombok.Value;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.SomeAggregate;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventsourcing.EventSourcingRepository;

@Value
public class SomeAggregateCommandHandler {

	private EventSourcingRepository<SomeAggregate> someAggregateEventSourcingRepository;

	@CommandHandler
	public void create(SomeAggregateCreateCommand someAggregateCreateCommand) throws Exception {
		someAggregateEventSourcingRepository.newInstance(() -> new SomeAggregate(someAggregateCreateCommand.getSomeAggregateId()));
	}
}
