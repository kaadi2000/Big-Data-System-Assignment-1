package de.ddm.actors.profiling;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		int task;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static class ValidateIndBatchMessage implements Message {
		public final List<String[]> dependentColumn;
        public final List<String[]> referencedColumn;
        public final ActorRef<DependencyMiner.Message> replyTo;

		public ValidateIndBatchMessage(List<String[]> dependentColumn, List<String[]> referencedColumn, ActorRef<DependencyMiner.Message> replyTo) {
			try {
				if (dependentColumn == null || referencedColumn == null){
					throw new IllegalArgumentException("Invalid data in ValidateIndMessage: Null values!");
				}
				if (dependentColumn.isEmpty() || referencedColumn.isEmpty()){
					throw new IllegalArgumentException("Invalid data in ValidateIndMessage: Empty columns!");
				}
			}
			catch (IllegalArgumentException e){
				e.printStackTrace();
				System.out.println("Invalid data in ValidateIndMessage: Null or empty columns!");
			}
			this.dependentColumn = dependentColumn;
            this.referencedColumn = referencedColumn;
            this.replyTo = replyTo;
        }
	}
	
	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;


	
	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.onMessage(ValidateIndBatchMessage.class, this::handleValidateIndBatch)
				.build();
	}
	

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
		return this;
	}



	private Behavior<Message> handle(TaskMessage message) {
		this.getContext().getLog().info("Working!");
		this.getContext().getLog().info("Working on task ID: {}", message.getTask());

		// I should probably know how to solve this task, but for now I just pretend some work...

		int result = message.getTask();
		long time = System.currentTimeMillis();
		Random rand = new Random();
		int runtime = (rand.nextInt(2) + 2) * 1000;
		while (System.currentTimeMillis() - time < runtime)
			result = ((int) Math.abs(Math.sqrt(result)) * result) % 1334525;

		LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), result);
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));

		return this;
	}

	private Behavior<Message> handleValidateIndBatch(ValidateIndBatchMessage message) {
		this.getContext().getLog().info("IND validation for dependent and referenced columns...");

		Set<String> referencedSet = new HashSet<>();
		for (String[] row : message.referencedColumn) {
			referencedSet.add(row[0]);
		}

		boolean isValidInd = true;
		for (String[] row : message.dependentColumn) {
			if (!referencedSet.contains(row[0])) {
				isValidInd = false;
				break;
			}
		}

		if (isValidInd) {
			this.getContext().getLog().info("IND validated successfully!");
			message.replyTo.tell(new DependencyMiner.CompletionMessage(this.getContext().getSelf(), 1)); // Success
		} else {
			this.getContext().getLog().info("Could not validate IND!");
			message.replyTo.tell(new DependencyMiner.CompletionMessage(this.getContext().getSelf(), 0)); // Failure
		}

		return this;
	}
}
