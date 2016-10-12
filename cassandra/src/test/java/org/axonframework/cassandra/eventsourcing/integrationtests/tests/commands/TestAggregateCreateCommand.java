package org.axonframework.cassandra.eventsourcing.integrationtests.tests.commands;

import lombok.Value;
import org.axonframework.commandhandling.TargetAggregateIdentifier;

import java.util.UUID;

/**
 * @author Nikola Yovchev
 */
@Value
public class TestAggregateCreateCommand {
	@TargetAggregateIdentifier
	private UUID testAggregateId;
}
