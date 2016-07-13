package org.axonframework.eventsourcing;

import org.axonframework.eventsourcing.eventstore.DomainEventStream;

/**
 * @author Joris van der Kallen
 * @since 3.0
 */
public class Conflict {

    private String aggregateIdentifier;
    private long actualVersion;
    private Long expectedVersion;
    private DomainEventStream conflictingEvents;
    private boolean resolved = false;

    public Conflict(String aggregateIdentifier, long actualVersion) {
        this(aggregateIdentifier, actualVersion, null, DomainEventStream.of(), true);
    }

    public Conflict(String aggregateIdentifier, long actualVersion, Long expectedVersion,
                    DomainEventStream conflictingEvents, boolean resolved) {
        this.aggregateIdentifier = aggregateIdentifier;
        this.actualVersion = actualVersion;
        this.expectedVersion = expectedVersion;
        this.conflictingEvents = conflictingEvents;
        this.resolved = resolved;
    }

    public String getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    public long getActualVersion() {
        return actualVersion;
    }

    public Long getExpectedVersion() {
        return expectedVersion;
    }

    public DomainEventStream getConflictingEvents() {
        return conflictingEvents;
    }

    public void markResolved() {
        resolved = true;
    }

    public boolean isResolved() {
        return resolved;
    }
}
