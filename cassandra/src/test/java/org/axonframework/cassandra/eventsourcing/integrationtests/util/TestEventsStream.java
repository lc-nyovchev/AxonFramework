package org.axonframework.cassandra.eventsourcing.integrationtests.util;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestEventsStream {

	public static List<EventEntry<?>> getEventEntries(int size) {
		List<EventEntry<?>> eventEntries = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			eventEntries.add(new EventEntry<>(
					UUID.randomUUID().toString(),
					Instant.now(),
					UUID.randomUUID().toString(),
					String.class
			));
		}
		return eventEntries;
	}
}
