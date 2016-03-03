import com.mapr.examples.telemetryagent.CarStreamsRouter;
import com.mapr.examples.telemetryagent.CarStreamConsumer;
import com.mapr.examples.telemetryagent.EventsStreamConsumer;
import com.mapr.examples.telemetryagent.TelemetryProducer;
import com.mapr.examples.telemetryagent.test.TestConsumer;
import com.mapr.examples.telemetryagent.test.TestProducer;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * Main class that executes different types of producers and consumers.
 */
public class Main {

    private final static String LOG_PATH = "/usr/local/share/games/torcs/telemetry/Inferno.dat";
    private final static int READ_TIMEOUT = 200;
    public static final int MAX_CARS_COUNT = 10;

    public static void main(String[] args) throws IOException {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("MapR Race Telemetry Example")
                .defaultHelp(true)
                .description("Simple Kafka client");
        parser.addArgument("-t", "--type")
                .choices("producer", "consumer", "router", "events", "test1", "test2")
                .required(true)
                .help("Specify client type");
        parser.addArgument("-c", "--conf")
                .required(true)
                .help("Path to file with properties in java format");
        parser.addArgument("-r", "--remove-log")
                .required(false)
                .help("Removes log file on start")
                .setDefault(false)
                .type(Boolean.class);
        Namespace res = parser.parseArgsOrFail(args);
        String type = res.get("type");
        final String confFilePath = res.get("conf");
        switch (type) {
            case "producer": {
                TelemetryProducer TelemetryProducer = new TelemetryProducer(confFilePath, LOG_PATH, READ_TIMEOUT);
                TelemetryProducer.start();
                break;
            }
            case "router": {
                CarStreamsRouter consumer = new CarStreamsRouter(confFilePath);
                consumer.start();
                break;
            }
            case "consumer": {
                ExecutorService service = Executors.newFixedThreadPool(MAX_CARS_COUNT);
                IntStream.range(1,MAX_CARS_COUNT).forEach((i) ->
                                    service.submit((Runnable) () -> {
                                        CarStreamConsumer carStreamConsumer = new CarStreamConsumer(confFilePath, i);
                                        carStreamConsumer.start();
                                    }));

//                CarStreamConsumer carStreamConsumer = new CarStreamConsumer(confFilePath, 4);
//                carStreamConsumer.start();

            }
            case "events": {
                EventsStreamConsumer eventsConsumer = new EventsStreamConsumer(confFilePath);
                eventsConsumer.start();
                break;
            }

            case "test1": {
                TestProducer test = new TestProducer();
                test.start();
                break;
            }
            case "test2": {
                TestConsumer test = new TestConsumer();
                test.start();
                break;
            }
            default:
                throw new IllegalArgumentException("Wrong client type: " + type);
        }
    }
}
