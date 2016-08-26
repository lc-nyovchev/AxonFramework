package org.axonframework.cassandra.eventsourcing.integrationtests.tests;

import lombok.extern.slf4j.Slf4j;
import org.axonframework.cassandra.eventsourcing.eventstore.CassandraTrackingToken;
import org.axonframework.cassandra.eventsourcing.integrationtests.config.AxonTestContext;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.commands.SomeAggregateCreateCommand;
import org.axonframework.cassandra.eventsourcing.integrationtests.util.EventEntry;
import org.axonframework.cassandra.eventsourcing.integrationtests.util.TestEventsStream;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.commandhandling.gateway.DefaultCommandGateway;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = AxonTestContext.class)
public class SampleTest {

	@Autowired private CommandGateway commandGateway;
	@Autowired private EventStorageEngine eventStorageEngine;

	@Test
	public void testWriteRead(){
		List<EventEntry<?>> eventEntries = TestEventsStream.getEventEntries(10);

		List<String> ids = eventEntries.stream()
				.map(EventEntry::getIdentifier)
				.collect(toList());

		eventStorageEngine.appendEvents(eventEntries);
		Stream<? extends TrackedEventMessage<?>> stream = eventStorageEngine.readEvents(new CassandraTrackingToken(ids.get(0), eventEntries.get(0).getTimestamp().toEpochMilli()), false);
		DomainEventStream eventStream = eventStorageEngine.readEvents(ids.get(0));
		eventStream.asStream().forEach(message -> {
			log.debug("Raw {}", message);
		});
		stream.forEach(trackedMessage -> {
			log.debug("Tracked {}", trackedMessage);
		});
	}

	@Test
	public void testCommandBus() throws InterruptedException {
		SomeAggregateCreateCommand someAggregateCreateCommand = new SomeAggregateCreateCommand(UUID.randomUUID());
		commandGateway.sendAndWait(someAggregateCreateCommand);
		DomainEventStream eventStream = eventStorageEngine.readEvents(someAggregateCreateCommand.getSomeAggregateId().toString());
		eventStream.asStream().forEach(message -> {
			log.debug("Raw {}", message);
		});
	}
}
