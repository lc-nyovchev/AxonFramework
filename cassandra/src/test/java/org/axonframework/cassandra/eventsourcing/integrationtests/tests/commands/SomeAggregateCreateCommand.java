package org.axonframework.cassandra.eventsourcing.integrationtests.tests.commands;

import lombok.Value;
import org.axonframework.commandhandling.TargetAggregateIdentifier;

import java.util.UUID;

@Value
public class SomeAggregateCreateCommand {
	@TargetAggregateIdentifier
	private UUID someAggregateId;
}
