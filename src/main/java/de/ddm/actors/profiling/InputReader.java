package de.ddm.actors.profiling;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

public class InputReader extends AbstractBehavior<InputReader.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable {}

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReadHeaderMessage implements Message {
        private static final long serialVersionUID = 1729062814525657711L;
        ActorRef<DependencyMiner.Message> replyTo;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReadBatchMessage implements Message {
        private static final long serialVersionUID = -7915854043207237318L;
        ActorRef<DependencyMiner.Message> replyTo;
        int batchSize;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EndOfFileMessage implements Message {
        private static final long serialVersionUID = 123456789L;
        ActorRef<DependencyMiner.Message> replyTo;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "inputReader";

    public static Behavior<Message> create(final int id, final File inputFile) {
        return Behaviors.setup(context -> new InputReader(context, id, inputFile));
    }

    private InputReader(ActorContext<Message> context, final int id, final File inputFile) throws IOException, CsvValidationException {
        super(context);
        if (inputFile == null || !inputFile.exists()) {
            throw new IllegalArgumentException("Invalid input file: " + inputFile);
        }

        this.id = id;
        this.reader = InputConfigurationSingleton.get().createCSVReader(inputFile);
        this.header = InputConfigurationSingleton.get().getHeader(inputFile);
        this.cachedRows = new ArrayList<>();
        this.eofReached = false;

        if (InputConfigurationSingleton.get().isFileHasHeader()) {
            String[] headerRow = this.reader.readNext();
            getContext().getLog().info("Extracted header for file {}: {}", inputFile.getName(), String.join(", ", headerRow));
        }
    }

    /////////////////
    // Actor State //
    /////////////////

    private final int id;
    private final CSVReader reader;
    private final String[] header;
    private final List<String[]> cachedRows;
    private boolean eofReached;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReadHeaderMessage.class, this::handle)
                .onMessage(ReadBatchMessage.class, this::handle)
                .onSignal(PostStop.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(ReadHeaderMessage message) {
        message.getReplyTo().tell(new DependencyMiner.HeaderMessage(this.id, this.header));
        getContext().getLog().info("Header sent for file ID {}: {}", this.id, String.join(", ", this.header));
        return this;
    }

    private Behavior<Message> handle(ReadBatchMessage message) throws IOException, CsvValidationException {
        List<String[]> batch = new ArrayList<>(message.getBatchSize());
        int linesRead = 0;

        for (int i = 0; i < message.getBatchSize(); i++) {
            String[] line = this.reader.readNext();
            if (line == null){
				break;
			}

            if(isBlankRow(cleanRow(line))){
                continue;
            }

            batch.add(line);
            linesRead++;
        }

        if (!batch.isEmpty()) {
            message.getReplyTo().tell(new DependencyMiner.BatchMessage(this.id, batch));
            getContext().getLog().info("Batch sent for file ID {}: {} rows", this.id, linesRead);
        } else {
            getContext().getLog().info("No more rows to read for file ID {}", this.id);
        }

        if(eofReached) {
            message.getReplyTo().tell(new DependencyMiner.EndOfFileMessage(this.id, message.getReplyTo()));
            getContext().getLog().info("End of file reached for file ID {}", this.id);
        }

        return this;
    }

    private String[] cleanRow(String[] row) {
        for (int i = 0; i < row.length; i++) {
            if (row[i] != null) {
                row[i] = row[i].trim(); // Remove leading and trailing spaces
            }
        }
        return row;
    }
    
    private boolean isBlankRow(String[] row) {
        for (String value : row) {
            if (value != null && !value.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private Behavior<Message> handle(PostStop signal) throws IOException {
        try {
            this.reader.close();
            getContext().getLog().info("Closed CSVReader for file ID {}", this.id);
        } catch (IOException e) {
            getContext().getLog().error("Error closing CSVReader for file ID {}: {}", this.id, e.getMessage());
        }
        return this;
    }
}
