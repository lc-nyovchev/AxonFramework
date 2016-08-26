package org.axonframework.cassandra.eventsourcing.integrationtests.config;

import com.datastax.driver.core.Cluster;
import org.axonframework.cassandra.eventsourcing.eventstore.CassandraEventStorageEngine;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.SomeAggregate;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.commands.SomeAggegateNameChangeCommand;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.commands.SomeAggregateCommandHandler;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.commands.SomeAggregateCreateCommand;
import org.axonframework.commandhandling.AggregateAnnotationCommandHandler;
import org.axonframework.commandhandling.AnnotationCommandHandlerAdapter;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.commandhandling.gateway.DefaultCommandGateway;
import org.axonframework.eventsourcing.EventSourcingRepository;
import org.axonframework.eventsourcing.eventstore.AbstractEventStore;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.TrackingEventStream;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.monitoring.MultiMessageMonitor;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import static org.mockito.Mockito.spy;

@Configuration
@Import(CassandraConfig.class)
public class AxonTestContext {

	@Bean
	public Serializer serializer(){
		return new XStreamSerializer();
	}

	@Bean
	public EventStorageEngine eventStorageEngine(Cluster cluster, Serializer serializer){
		return new CassandraEventStorageEngine(cluster, serializer);
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
	public EventSourcingRepository<SomeAggregate> someAggregateCreateCommandEventSourcingRepository(EventStore eventStore){
		return new EventSourcingRepository<>(SomeAggregate.class, eventStore);
	}

	@Bean
	public CommandBus commandBus(EventSourcingRepository<SomeAggregate> someAggregateEventSourcingRepository){
		CommandBus commandBus = new SimpleCommandBus();
		AnnotationCommandHandlerAdapter annotationCommandHandlerAdapter = new AnnotationCommandHandlerAdapter(
			new SomeAggregateCommandHandler(someAggregateEventSourcingRepository)
		);
		commandBus.subscribe(SomeAggregateCreateCommand.class.getName(), annotationCommandHandlerAdapter);
		commandBus.subscribe(SomeAggegateNameChangeCommand.class.getName(), annotationCommandHandlerAdapter);
		return commandBus;
	}

	@Bean
	public CommandGateway commandGateway(CommandBus commandBus){
		return new DefaultCommandGateway(commandBus);
	}
}
