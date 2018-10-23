package apoc.broker;

import apoc.ApocConfiguration;
import apoc.Pools;
import apoc.broker.logging.BrokerLogger;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class BrokerIntegration
{

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.send(connectionName, message, configuration) - Send a message to the broker associated with the connectionName namespace. Takes in parameter which are dependent on the broker being used." )
    public Stream<BrokerMessage> send( @Name( "connectionName" ) String connectionName, @Name( "message" ) Map<String,Object> message,
            @Name( "configuration" ) Map<String,Object> configuration ) throws Exception
    {

        return BrokerHandler.sendMessageToBrokerConnection( connectionName, message, configuration );
    }

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.receive(connectionName, configuration) - Receive a message from the broker associated with the connectionName namespace. Takes in a configuration map which is dependent on the broker being used." )
    public Stream<BrokerResult> receive( @Name( "connectionName" ) String connectionName, @Name( "configuration" ) Map<String,Object> configuration )
            throws IOException
    {

        return BrokerHandler.receiveMessageFromBrokerConnection( connectionName, configuration );
    }

    public enum BrokerType
    {
        RABBITMQ,
        SQS,
        KAFKA
    }

    public static class BrokerHandler
    {
        private static Map<String,BrokerConnection> brokerConnections;
        private static Log neo4jLog;

        public BrokerHandler( Map<String,BrokerConnection> brokerConnections, Log log )
        {
            this.brokerConnections = brokerConnections;
            neo4jLog = log;
        }

        public static Stream<BrokerMessage> sendMessageToBrokerConnection( String connection, Map<String,Object> message, Map<String,Object> configuration )
                throws Exception
        {
            if ( !brokerConnections.containsKey( connection ) )
            {
                throw new IOException( "Broker Exception. Connection '" + connection + "' is not a configured broker connection." );
            }
            try {
                (brokerConnections.get( connection )).checkConnectionHealth();
                Stream<BrokerMessage> brokerMessageStream = (brokerConnections.get( connection )).send( message, configuration );

                Pools.DEFAULT.execute( (Runnable) () -> retryMessagesForConnectionAsynch( connection ) );


                return brokerMessageStream;
            }
            catch ( Exception e )
            {
                BrokerLogger.error( new BrokerLogger.LogLine.LogEntry( connection, message, configuration ) );
                ConnectionManager.asyncReconnect( connection, neo4jLog);

                if (BrokerLogger.IsAtThreshold())
                {
                    retryMessagesAsynch();
                }
            }
            throw new RuntimeException( "Unable to send message to connection '" + connection + "'. Logged in '" + BrokerLogger.getLogName() + "'." );
        }

        public static Stream<BrokerResult> receiveMessageFromBrokerConnection( String connection, Map<String,Object> configuration ) throws IOException
        {
            if ( !brokerConnections.containsKey( connection ) )
            {
                throw new IOException( "Broker Exception. Connection '" + connection + "' is not a configured broker connection." );
            }
            return brokerConnections.get( connection ).receive( configuration );
        }

        public static void retryMessagesForConnectionAsynch( String connectionName )
        {
            try
            {
                neo4jLog.info( "APOC Broker: Resending messages for '" + connectionName + "'." );
                List<BrokerLogger.LogLine> linesToRemove = new ArrayList<>(  );
                BrokerLogger.streamLogLines( connectionName ).parallel().forEach( (ll) ->
                {
                    BrokerLogger.LogLine.LogEntry logEntry = ll.getLogEntry();
                    Boolean b = resendBrokerMessage( logEntry.getConnectionName(), logEntry.getMessage(), logEntry.getConfiguration() );
                    if(b)
                    {
                        //Send successfull. Now delete.
                        linesToRemove.add( ll );
                    }

                });
                if (!linesToRemove.isEmpty())
                {
                    BrokerLogger.removeLogLineBatch( linesToRemove );
                }
            }
            catch ( Exception e )
            {

            }

        }

        public static void retryMessagesAsynch( )
        {
            try
            {
                neo4jLog.info( "APOC Broker: Logger has reached its message limit. Resending messages for all connections." );
                List<BrokerLogger.LogLine> linesToRemove = new ArrayList<>(  );
                BrokerLogger.streamLogLines( ).parallel().forEach( (ll) ->
                {
                    BrokerLogger.LogLine.LogEntry logEntry = ll.getLogEntry();
                    Boolean b = resendBrokerMessage( logEntry.getConnectionName(), logEntry.getMessage(), logEntry.getConfiguration() );
                    if(b)
                    {
                        //Send successfull. Now delete.
                        linesToRemove.add( ll );
                    }
                    if(linesToRemove.size() > 10)
                    {
                        BrokerLogger.removeLogLineBatch( linesToRemove );
                        linesToRemove.clear();
                    }

                });
                if (!linesToRemove.isEmpty())
                {
                    BrokerLogger.removeLogLineBatch( linesToRemove );
                }
            }
            catch ( Exception e )
            {

            }

        }

        public static Boolean resendBrokerMessage( String connection, Map<String,Object> message, Map<String,Object> configuration )
        {
            if ( !brokerConnections.containsKey( connection ) )
            {
                throw new RuntimeException( "Broker Exception. Connection '" + connection + "' is not a configured broker connection." );
            }
            try
            {
                (brokerConnections.get( connection )).send( message, configuration );
            }
            catch (Exception e )
            {
                return false;
            }
            return true;
        }

        public static synchronized void setBrokerConnections( Map<String,BrokerConnection> brokerConnections)
        {
            BrokerHandler.brokerConnections = brokerConnections;
        }

    }

    public static class BrokerLifeCycle
    {
        private final Log log;
        private final GraphDatabaseAPI db;

        private static final String LOGS_CONFIG = "logs";

        public BrokerLifeCycle(  GraphDatabaseAPI db, Log log)
        {
            this.log = log;
            this.db = db;
        }

        private static String getBrokerConfiguration( String connectionName, String key )
        {
            Map<String,Object> value = ApocConfiguration.get( "broker." + connectionName );

            if ( value == null )
            {
                throw new RuntimeException( "No apoc.broker." + connectionName + " specified" );
            }
            return (String) value.get( key );
        }

        public void start()
        {
            Map<String,Object> value = ApocConfiguration.get( "broker" );

            Set<String> connectionList = new HashSet<>();

            value.forEach( ( configurationString, object ) -> {
                String connectionName = configurationString.split( "\\." )[0];

                if ( connectionName.equals( LOGS_CONFIG ) )
                {
                    BrokerLogger.initializeBrokerLogger( db, ApocConfiguration.get( "broker." + LOGS_CONFIG ) );
                }
                else
                {
                    connectionList.add( connectionName );
                }
            } );

            for ( String connectionName : connectionList )
            {

                BrokerType brokerType = BrokerType.valueOf( StringUtils.upperCase( getBrokerConfiguration( connectionName, "type" ) ) );
                Boolean enabled = Boolean.valueOf( getBrokerConfiguration( connectionName, "enabled" ) );

                if ( enabled )
                {
                    switch ( brokerType )
                    {
                    case RABBITMQ:
                        ConnectionManager.addRabbitMQConnection( connectionName, log, ApocConfiguration.get( "broker." + connectionName ) );
                        break;
                    case SQS:
                        ConnectionManager.addSQSConnection( connectionName, log, ApocConfiguration.get( "broker." + connectionName ) );
                        break;
                    case KAFKA:
                        ConnectionManager.addKafkaConnection( connectionName, log, ApocConfiguration.get( "broker." + connectionName ) );
                        break;
                    default:
                        break;
                    }
                }
            }

            new BrokerHandler( ConnectionManager.getBrokerConnections(), log );
        }

        public void stop()
        {
            ConnectionManager.closeConnections();
        }
    }
}
