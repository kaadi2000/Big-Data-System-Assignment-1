package de.ddm.actors.profiling;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.actors.profiling.DependencyWorker.ValidateIndBatchMessage;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////d
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	private boolean tasksPending() {
		return System.currentTimeMillis() - this.startTime < 2000000;
	}
	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		InclusionDependency result;
	}

	private int getNextTaskId() {
        return new Random().nextInt(100);
    }


	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private boolean discoveryComplete() {
		return !tasksPending();
	}
	
	private InclusionDependency createRandomInd() {
		Random random = new Random();
		int dependent = random.nextInt(this.inputFiles.length);
		int referenced = random.nextInt(this.inputFiles.length);
		File dependentFile = this.inputFiles[dependent];
		File referencedFile = this.inputFiles[referenced];
		String[] dependentAttributes = {this.headerLines[dependent][random.nextInt(this.headerLines[dependent].length)]};
		String[] referencedAttributes = {this.headerLines[referenced][random.nextInt(this.headerLines[referenced].length)]};
		return new InclusionDependency(dependentFile, dependentAttributes, referencedFile, referencedAttributes);
	}

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		getContext().getLog().info("Received header from input reader {}", message.getId());

		if(allHeadersReceived()){
			this.getContext().getLog().info("All headers received!");
			try {
				generateCandidates();
				this.getContext().getLog().info("Canditates generation succefully completed!");
			}
			catch (Exception e) {
				this.getContext().getLog().error("Error while generating candidates: {}", e.getMessage());
			}
		}

		return this;
	}

	private boolean allHeadersReceived() {
		for (String[] headerLine : this.headerLines){
			if (headerLine == null){
				return false;
			}
		}
		return true;
	}

	private void generateCandidates() {
		for (int i = 0; i < inputFiles.length; i++) {
			for (int j = 0; j < inputFiles.length; j++) {
				if (i == j){
					continue;
				}
	
				for (String dependentColumn : this.headerLines[i]) {
					for (String referencedColumn : this.headerLines[j]) {
						if (!dependentColumn.isEmpty() && !referencedColumn.isEmpty()) {
							sendValidationTask(i, dependentColumn, j, referencedColumn);
						}
					}
				}
			}
		}
	}

	private int workerIndex = 0;
	
	private void sendValidationTask(int dependentFileId, String dependentColumn, 
                                int referencedFileId, String referencedColumn) {
		getContext().getLog().info("Preparing task for validation: {}[{}] -> {}[{}]", inputFiles[dependentFileId].getName(), dependentColumn, inputFiles[referencedFileId].getName(), referencedColumn);

		// Get the next worker in round-robin fashion
		ActorRef<DependencyWorker.Message> worker = this.dependencyWorkers.get(workerIndex);
		workerIndex = (workerIndex + 1) % this.dependencyWorkers.size();

		worker.tell(new DependencyWorker.TaskMessage(
			this.largeMessageProxy, inputFiles[dependentFileId], dependentColumn, inputFiles[referencedFileId], referencedColumn));

	}





	private Behavior<Message> handle(BatchMessage message) {
		// Ignoring batch content for now ... but I could do so much with it.

//		System.out.println(MemoryUtils.byteSizeOf(message.getBatch()));
//		System.out.println(MemoryUtils.bytesMax() + "    " + MemoryUtils.bytesFree());

		if (!message.getBatch().isEmpty() && !this.dependencyWorkers.isEmpty()) {
			String[] dependentColumns = this.headerLines[0];
			String[] referencedColumns = this.headerLines[1];

			for (String dependentColumn : dependentColumns) {
				for (String referencedColumn : referencedColumns) {
					ActorRef<DependencyWorker.Message> worker = this.dependencyWorkers.get(0);
					worker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, inputFiles[0], dependentColumn, inputFiles[1], referencedColumn));
				}
			}
		}
		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
			// The worker should get some work ... let me send her something before I figure out what I actually want from her.
			// I probably need to idle the worker for a while, if I do not have work for it right now ... (see master/worker pattern)
			
			if(allHeadersReceived()){
				generateCandidates();
			}

			//dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, this.getNextTaskId()));
		}
		return this;
	}

	private Behavior<Message> handleValidateIndBatch(ValidateIndBatchMessage message) {
		this.getContext().getLog().info("Validating IND for dependent field ID: {} and referenced field ID: {}", message.dependentFile, message.referencedFile);
	

		Set<String> referencedSet = new HashSet<>();
		for (String[] row : message.referencedColumn) {
			if (row[0] != null) {
				referencedSet.add(row[0].trim());
			}
		}


		boolean isValidInd = true;
		for (String[] row : message.dependentColumn) {
			if (row[0] == null || !referencedSet.contains(row[0].trim())) {
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
	

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		// If this was a reasonable result, I would probably do something with it and potentially generate more work ... for now, let's just generate a random, binary IND.

		if (message.getResult() != null) {
			InclusionDependency ind = message.getResult();
			this.getContext().getLog().info("Worker {} produced a valid IND: {}", dependencyWorker, ind);
	
			List<InclusionDependency> inds = new ArrayList<>(1);
			inds.add(ind);
			this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
		} else {
			this.getContext().getLog().info("Worker {} reported no valid IND.", dependencyWorker);
		}
	
		if (tasksPending()) {
			dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, inputFiles[0],"dependentColumnName", inputFiles[1], "referencedColumnName"));
		} else {
			getContext().getLog().info("No more tasks pending for Worker {}", dependencyWorker);
		}
	
		if (discoveryComplete()) {
			this.end();
		}
	
		return this;
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);

		for (ActorRef<InputReader.Message> reader : inputReaders) {
			getContext().stop(reader);
		}
		for (ActorRef<DependencyWorker.Message> worker : dependencyWorkers) {
			getContext().stop(worker);
		}
		getContext().stop(this.getContext().getSelf());
	}


	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);
		return this;
	}
}