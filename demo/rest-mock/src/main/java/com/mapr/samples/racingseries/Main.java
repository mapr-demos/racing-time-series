package com.mapr.samples.racingseries;

import com.mapr.examples.telemetryagent.CarsDAO;
import com.mapr.examples.telemetryagent.util.NoRacesException;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;

public class Main {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) throws Exception {
        LOG.info("================================================");
        LOG.info("   Starting Telemetry UI");
        LOG.info("================================================\n\n");
        try {
            //Init database connection
            new CarsDAO().getLatestRace();
        } catch (NoRacesException ex) {
            System.err.println("No races found");
        }

        Server server = new Server(8080);


        ServletHolder sh = new ServletHolder(ServletContainer.class);
        // Set the package where the services reside
        sh.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "com.mapr.samples.racingseries.api");

        sh.setInitOrder(1); // force loading at startup

        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setDirectoriesListed(true);
        resourceHandler.setResourceBase("./src/main/resources/webapp");

        ServletContextHandler sch = new ServletContextHandler();
        sch.addServlet(sh, "/*");

        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[]{resourceHandler, sch});
        server.setHandler(handlers);

        server.start();
        server.join();
    }


}