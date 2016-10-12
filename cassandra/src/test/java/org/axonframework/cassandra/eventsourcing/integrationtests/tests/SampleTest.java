package org.axonframework.cassandra.eventsourcing.integrationtests.tests;

import lombok.extern.slf4j.Slf4j;
import org.axonframework.cassandra.eventsourcing.eventstore.CassandraTrackingToken;
import org.axonframework.cassandra.eventsourcing.integrationtests.config.AxonTestContext;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.commands.TestAggregateCreateCommand;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.commands.TestAggregateNameChangeCommand;
import org.axonframework.cassandra.eventsourcing.integrationtests.util.EventEntry;
import org.axonframework.cassandra.eventsourcing.integrationtests.util.TestEventsStream;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.axonframework.commandhandling.GenericCommandMessage.asCommandMessage;

/**
 * @author Nikola Yovchev
 */
@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = AxonTestContext.class)
public class SampleTest {

	@Autowired private CommandBus commandBus;
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
	public void testEventSourcing() throws InterruptedException {
		TestAggregateCreateCommand testAggregateCreateCommand = new TestAggregateCreateCommand(UUID.randomUUID());

		Map<String, Object> someMetadata = new HashMap<>();
		someMetadata.put("someStringMeta", "someString");
		someMetadata.put("someLongMeta", 1337L);

		commandBus.dispatch(asCommandMessage(testAggregateCreateCommand).withMetaData(someMetadata));
		commandBus.dispatch(asCommandMessage(new TestAggregateNameChangeCommand(testAggregateCreateCommand.getTestAggregateId(), "testNameChange2")));
		commandBus.dispatch(asCommandMessage(new TestAggregateNameChangeCommand(testAggregateCreateCommand.getTestAggregateId(), "testNameChange3")));
		commandBus.dispatch(asCommandMessage(new TestAggregateNameChangeCommand(testAggregateCreateCommand.getTestAggregateId(), "testNameChange4")));
		commandBus.dispatch(asCommandMessage(new TestAggregateNameChangeCommand(testAggregateCreateCommand.getTestAggregateId(), "testNameChange6")));
		DomainEventStream eventStream = eventStorageEngine.readEvents(testAggregateCreateCommand.getTestAggregateId().toString());

		eventStream.asStream().forEach(message -> {
			log.debug("Raw {}", message);
		});

	}
}
