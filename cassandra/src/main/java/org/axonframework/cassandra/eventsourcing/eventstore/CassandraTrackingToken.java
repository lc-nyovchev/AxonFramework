package org.axonframework.cassandra.eventsourcing.eventstore;

import lombok.Value;
import org.axonframework.eventsourcing.eventstore.TrackingToken;

import static java.util.Comparator.comparing;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsFirst;

/**
 * A token that provides no guarantee on nexts but provides guarantee on comparison
 * @author Nikola Yovchev
 */
@Value
public class CassandraTrackingToken implements TrackingToken {

	private final String aggregateIdentifier;
	private final Long timestamp;

	@Override
	public boolean isGuaranteedNext(TrackingToken otherToken) {
		if (!(otherToken instanceof CassandraTrackingToken)){
			throw new IllegalArgumentException("Other token wasn't a CassandraTrackingToken");
		}
		return false;
	}

	@Override
	public int compareTo(TrackingToken otherToken) {
		if (!(otherToken instanceof CassandraTrackingToken)){
			throw new IllegalArgumentException("Other token wasn't a CassandraTrackingToken");
		}
		CassandraTrackingToken otherTokenAsCassandraToken = (CassandraTrackingToken) otherToken;
		return comparing(CassandraTrackingToken::getTimestamp,
			nullsFirst(naturalOrder()
		)).compare(this, otherTokenAsCassandraToken);
	}
}
