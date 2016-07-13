package org.axonframework.eventsourcing;

import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.commandhandling.model.ConflictingAggregateVersionException;
import org.axonframework.commandhandling.model.Repository;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.messaging.unitofwork.DefaultUnitOfWork;
import org.junit.*;
import org.mockito.*;

import static org.axonframework.commandhandling.GenericCommandMessage.asCommandMessage;

public class EventSourcingRepositoryTest_ConflictDetection {

    private EventStore eventStore;
    private Repository<StubAggregate> repository;

    @Before
    public void setUp() throws Exception {
        eventStore = Mockito.mock(EventStore.class);
        Mockito.when(eventStore.readEvents(Matchers.anyString())).thenAnswer(invocationOnMock -> DomainEventStream
                .of(new GenericDomainEventMessage<Object>("type", invocationOnMock.getArgumentAt(0, String.class), 1,
                                                          "Test1"),
                    new GenericDomainEventMessage<Object>("type", invocationOnMock.getArgumentAt(0, String.class), 2,
                                                          "Test2"),
                    new GenericDomainEventMessage<Object>("type", invocationOnMock.getArgumentAt(0, String.class), 3,
                                                          "Test3")));
        repository = new EventSourcingRepository<>(StubAggregate.class,
                                                   eventStore);
        DefaultUnitOfWork.startAndGet(asCommandMessage("Stub"));
    }

    @Test
    public void testLoadWithoutConflicts() throws Exception {
        repository.load("id");
        CurrentUnitOfWork.commit();
    }

    @Test(expected = ConflictingAggregateVersionException.class)
    public void testConflictDetection() throws Exception {
        repository.load("id", 2L);
        CurrentUnitOfWork.commit();
    }

    private static class StubAggregate {

        @AggregateIdentifier
        private String id;

        public StubAggregate() {
        }

        @CommandHandler
        public void handle(String command) {
        }
    }
}