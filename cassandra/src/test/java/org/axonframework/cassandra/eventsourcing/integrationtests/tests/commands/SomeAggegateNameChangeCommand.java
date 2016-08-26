package org.axonframework.cassandra.eventsourcing.integrationtests.tests.commands;

import lombok.Value;
import org.axonframework.commandhandling.TargetAggregateIdentifier;

import java.util.UUID;

@Value
public class SomeAggegateNameChangeCommand {
	@TargetAggregateIdentifier
	private UUID someAggregateId;
	private String name;
}
