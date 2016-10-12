package org.axonframework.cassandra.eventsourcing.integrationtests.tests.commands;

import lombok.Value;
import org.axonframework.commandhandling.TargetAggregateIdentifier;

import java.util.UUID;

/**
 * @author Nikola Yovchev
 */
@Value
public class TestAggregateNameChangeCommand {
	@TargetAggregateIdentifier
	private UUID testAggregateId;
	private String name;
}
