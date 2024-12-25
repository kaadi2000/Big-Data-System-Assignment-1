package de.ddm.actors.profiling;

import java.io.File;
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
import de.ddm.structures.InclusionDependency;
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

		File dependentFile;
		String dependentColumn;
	
		File referencedFile;
		String referencedColumn;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static class ValidateIndBatchMessage implements Message {
		public final File dependentFile;
		public final List<String[]> dependentColumn;
		public final File referencedFile;
		public final List<String[]> referencedColumn;
		public final ActorRef<DependencyMiner.Message> replyTo;

		public ValidateIndBatchMessage(File dependentFile, List<String[]> dependentColumn, 
									File referencedFile, List<String[]> referencedColumn, 
									ActorRef<DependencyMiner.Message> replyTo) {
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
			this.dependentFile = dependentFile;
			this.dependentColumn = dependentColumn;
			this.referencedFile = referencedFile;
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
		this.getContext().getLog().info("Validating IND: {}[{}] -> {}[{}]", message.getDependentFile().getName(), message.getDependentColumn(), message.getReferencedFile().getName(), message.getReferencedColumn());
	
		// Perform some processing (simulate work or validation logic)
		long time = System.currentTimeMillis();
		Random rand = new Random();
		int runtime = (rand.nextInt(2) + 2) * 1000;
		while (System.currentTimeMillis() - time < runtime) {
			// Simulate computation 
		}
		InclusionDependency realResult = new InclusionDependency(message.getDependentFile(), new String[]{message.getDependentColumn()}, message.getReferencedFile(), new String[]{message.getReferencedColumn()});
	
		LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), realResult);
	
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));
	
		return this;
	}

	private Behavior<Message> handleValidateIndBatch(ValidateIndBatchMessage message) {
		this.getContext().getLog().info("Validating IND for dependent field ID: {} and referenced field ID: {}", message.dependentFile, message.referencedFile);
	

		Set<String> referencedSet = new HashSet<>();
		for (String[] row : message.referencedColumn) {
			if (row[0] != null || !row[0].trim().isEmpty()) {
				referencedSet.add(row[0]);
			}
		}


		boolean isValidInd = true;
		for (String[] row : message.dependentColumn) {
			if (row[0] == null || row[0].trim().isEmpty() || !referencedSet.contains(row[0])) {
				isValidInd = false;
				break;
			}
		}
	
		InclusionDependency result = null;
		if (isValidInd) {
			result = new InclusionDependency(
				message.dependentFile,
				new String[]{message.dependentColumn.get(0)[0]},
				message.referencedFile,
				new String[]{message.referencedColumn.get(0)[0]}
			);
			this.getContext().getLog().info("IND validated successfully! {}", result);
		} else {
			this.getContext().getLog().info("Invalid IND for DependentFile: {} -> ReferencedFile: {}", message.dependentFile.getName(), message.referencedFile.getName());
		}
	
		message.replyTo.tell(new DependencyMiner.CompletionMessage(this.getContext().getSelf().unsafeUpcast(), result));
		return this;
	}

	
}
