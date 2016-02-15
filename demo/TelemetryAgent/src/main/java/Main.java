import com.mapr.examples.telemetryagent.CarStreamsRouter;
import com.mapr.examples.telemetryagent.Consumer;
import com.mapr.examples.telemetryagent.TelemetryProducer;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

public class Main {

    private final static String LOG_PATH = "/usr/local/share/games/torcs/telemetry/Inferno.dat";
    private final static int READ_TIMEOUT = 1000;

    public static void main(String[] args) {

        ArgumentParser parser = ArgumentParsers.newArgumentParser("Kafka client")
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
        String confFilePath = res.get("conf");
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
                Consumer consumer = new Consumer(confFilePath);
                consumer.start();
                break;
            }
            default:
                throw new IllegalArgumentException("Wrong client type: " + type);
        }
    }
}
