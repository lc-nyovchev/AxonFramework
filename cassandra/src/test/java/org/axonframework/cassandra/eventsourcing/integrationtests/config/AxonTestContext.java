package org.axonframework.cassandra.eventsourcing.integrationtests.config;

import com.datastax.driver.core.Cluster;
import org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageEngine;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.TestAggregate;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.commands.TestAggregateCreateCommand;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.commands.TestAggregateNameChangeCommand;
import org.axonframework.commandhandling.AggregateAnnotationCommandHandler;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.disruptor.DisruptorCommandBus;
import org.axonframework.eventsourcing.EventSourcingRepository;
import org.axonframework.eventsourcing.eventstore.AbstractEventStore;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.TrackingEventStream;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(CassandraTestConfig.class)
public class AxonTestContext {

	@Bean
	public EventStorageEngine eventStorageEngine(Cluster cluster){
		return new CassandraEventStorageEngine(cluster, new XStreamSerializer());
	}

	@Bean
	public EventStore eventStore(EventStorageEngine eventStorageEngine){
		return new AbstractEventStore(eventStorageEngine) {
			@Override
			public TrackingEventStream streamEvents(TrackingToken trackingToken) {
				return null;
			}
		};
	}

	@Bean
	public EventSourcingRepository<TestAggregate> aggregateEventSourcingRepository(EventStore eventStore){
		return new EventSourcingRepository<>(TestAggregate.class, eventStore);
	}

	@Bean
	public CommandBus commandBus(EventStore eventStore){
		CommandBus commandBus = new SimpleCommandBus();
		EventSourcingRepository<TestAggregate> testAggregateEventSourcingRepository = new EventSourcingRepository<>(TestAggregate.class, eventStore);
		AggregateAnnotationCommandHandler commandHandler = new AggregateAnnotationCommandHandler(TestAggregate.class, testAggregateEventSourcingRepository);
		commandBus.subscribe(TestAggregateCreateCommand.class.getName(), commandHandler);
		commandBus.subscribe(TestAggregateNameChangeCommand.class.getName(), commandHandler);
		return commandBus;
	}
}
