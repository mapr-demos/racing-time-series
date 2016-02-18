import com.mapr.examples.telemetryagent.CarStreamsRouter;
import com.mapr.examples.telemetryagent.CarStreamConsumer;
import com.mapr.examples.telemetryagent.TelemetryProducer;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * Main class that executes different types of producers and consumers.
 */
public class Main {

    private final static String LOG_PATH = "/usr/local/share/games/torcs/telemetry/Inferno.dat";
    private final static int READ_TIMEOUT = 1000;
    public static final int MAX_CARS_COUNT = 10;

    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("MapR Race Telemetry Example")
                .defaultHelp(true)
                .description("Simple Kafka client");
        parser.addArgument("-t", "--type")
                .choices("producer", "consumer", "router")
                .required(true)
                .help("Specify client type");
        parser.addArgument("-c", "--conf")
                .required(true)
                .help("Path to file with properties in java format");
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
            default:
                throw new IllegalArgumentException("Wrong client type: " + type);
        }
    }
}
