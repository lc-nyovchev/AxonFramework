package org.axonframework.cassandra.eventsourcing.integrationtests.tests;


import lombok.NoArgsConstructor;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.commands.TestAggregateCreateCommand;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.commands.TestAggregateNameChangeCommand;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.events.TestAggregateCreatedEvent;
import org.axonframework.cassandra.eventsourcing.integrationtests.tests.events.TestAggregateNameChangedEvent;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.commandhandling.model.AggregateIdentifier;
import org.axonframework.eventsourcing.EventSourcingHandler;
import org.axonframework.messaging.MetaData;

import java.util.UUID;

import static org.axonframework.commandhandling.model.AggregateLifecycle.apply;

/**
 * @author Nikola Yovchev
 */
@NoArgsConstructor
public class TestAggregate {

	@AggregateIdentifier private UUID someAggregateId;
	private String name;

	@CommandHandler
	public TestAggregate(TestAggregateCreateCommand testAggregateCreateCommand, MetaData metaData){
		apply(new TestAggregateCreatedEvent(testAggregateCreateCommand.getTestAggregateId()), metaData);
	}

	@CommandHandler
	public void setName(TestAggregateNameChangeCommand testAggregateNameChangeCommand, MetaData metaData){
		apply(new TestAggregateNameChangedEvent(
			testAggregateNameChangeCommand.getTestAggregateId(),
			testAggregateNameChangeCommand.getName()
		), metaData);
	}

	@EventSourcingHandler
	public void on(TestAggregateCreatedEvent createdEvent){
		this.someAggregateId = createdEvent.getSomeAggregateId();
	}

	@EventSourcingHandler
	public void on(TestAggregateNameChangedEvent testAggregateNameChangedEvent){
		this.name = testAggregateNameChangedEvent.getName();
	}
}
