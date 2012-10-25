package com.github.chirino.aqmpdemo.camelbridge;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;

import java.util.Arrays;
import java.util.LinkedList;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class CamelBridge {

    public static void main(String[] cmdline) throws Exception {
        CamelBridge bridge = new CamelBridge();
        LinkedList<String> args = new LinkedList<String>(Arrays.asList(cmdline));
        while( !args.isEmpty() ) {
            String arg = args.removeFirst();
            if( "--source-host".equals(arg) ) {
                bridge.sourceHost = removeOptionArg(args, arg);
            } else if( "--source-port".equals(arg) ) {
                bridge.sourcePort = Integer.parseInt(removeOptionArg(args, arg));
            } else if( "--source-ssl".equals(arg) ) {
                bridge.sourceSsl = true;
            } else if( "--source-user".equals(arg) ) {
                bridge.sourceUser = removeOptionArg(args, arg);
            } else if( "--source-password".equals(arg) ) {
                bridge.sourcePassword = removeOptionArg(args, arg);
            } else if( "--source-dest".equals(arg) ) {
                bridge.sourceDest = removeOptionArg(args, arg);
            } else if( "--target-host".equals(arg) ) {
                bridge.targetHost = removeOptionArg(args, arg);
            } else if( "--target-port".equals(arg) ) {
                bridge.targetPort = Integer.parseInt(removeOptionArg(args, arg));
            } else if( "--target-ssl".equals(arg) ) {
                bridge.targetSsl = true;
            } else if( "--target-user".equals(arg) ) {
                bridge.targetUser = removeOptionArg(args, arg);
            } else if( "--target-password".equals(arg) ) {
                bridge.targetPassword = removeOptionArg(args, arg);
            } else if( "--target-dest".equals(arg) ) {
                bridge.targetDest = removeOptionArg(args, arg);
            } else if( "--dest".equals(arg) ) {
                bridge.sourceDest = removeOptionArg(args, arg);
                bridge.targetDest = bridge.sourceDest;
            } else {
                invalidUsage("Unknown argument/option '" + arg + "'");
            }
        }
        bridge.start();
        while(true) {
            // Don't exit.
            Thread.sleep(100000);
        }
    }

    private static String removeOptionArg(LinkedList<String> args, String option) {
        if( args.isEmpty() ) {
            invalidUsage("Missing argument for option '"+option+"'.");
        }
        return args.removeFirst();
    }

    private static void invalidUsage(String msg) {
        System.err.println("Invalid Usage: "+msg);
        System.exit(-2);
    }


    DefaultCamelContext context;

    String sourceHost = "localhost";
    int sourcePort = 5672;
    boolean sourceSsl = false;
    String sourceUser = null;
    String sourcePassword = null;
    String sourceDest = "topic:transform_requests";

    String targetHost = "localhost";
    int targetPort = 5672;
    boolean targetSsl = false;
    String targetUser = null;
    String targetPassword = null;
    String targetDest = "topic:transform_requests";

    public void start() throws Exception {
        context = new DefaultCamelContext();
        JmsComponent source = new JmsComponent();
        source.setConnectionFactory(new ConnectionFactoryImpl(sourceHost, sourcePort, sourceUser, sourcePassword, null, sourceSsl));
        context.addComponent("amqp-source", source);

        JmsComponent target = new JmsComponent();
        target.setConnectionFactory(new ConnectionFactoryImpl(targetHost, targetPort, targetUser, targetPassword));
        context.addComponent("amqp-target", target);

        RouteBuilder builder = new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                //errorHandler(deadLetterChannel("mock:error"));
                from("amqp-source:"+sourceDest)
                    .to("log:test")
                    .to("amqp-target:"+targetDest);
                // from("timer://foo?fixedRate=true&period=5000").to("log:test");
            }
        };
        builder.addRoutesToCamelContext(context);
        context.start();
    }

}
