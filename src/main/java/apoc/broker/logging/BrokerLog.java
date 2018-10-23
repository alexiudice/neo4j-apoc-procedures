package apoc.broker.logging;

import org.neo4j.function.Suppliers;
import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.AbstractPrintWriterLogger;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;
import org.neo4j.logging.NullLogger;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

public class BrokerLog extends AbstractLog
{
    static final Supplier<Date> DEFAULT_CURRENT_DATE_SUPPLIER = Date::new;
    static final Function<OutputStream,PrintWriter> OUTPUT_STREAM_CONVERTER =
            outputStream -> new PrintWriter( new OutputStreamWriter( outputStream, StandardCharsets.UTF_8 ) );
    static final TimeZone UTC = TimeZone.getTimeZone( "UTC" );

    /**
     * A Builder for a {@link BrokerLog}
     */
    public static class Builder
    {
        private TimeZone timezone = UTC;
        private Object lock = this;
        private String category = null;
        private Level level = Level.INFO;
        private boolean autoFlush = true;

        private Builder()
        {
        }

        /**
         * Set the timezone for datestamps in the log
         *
         * @return this builder
         */
        public Builder withUTCTimeZone()
        {
            return withTimeZone( UTC );
        }

        /**
         * Set the timezone for datestamps in the log
         *
         * @param timezone the timezone to use for datestamps
         * @return this builder
         */
        public Builder withTimeZone( TimeZone timezone )
        {
            this.timezone = timezone;
            return this;
        }

        /**
         * Use the specified object to synchronize on.
         *
         * @param lock the object to synchronize on
         * @return this builder
         */
        public Builder usingLock( Object lock )
        {
            this.lock = lock;
            return this;
        }

        /**
         * Include the specified category in each output log line.
         *
         * @param category the category to include ing each output line
         * @return this builder
         */
        public Builder withCategory( String category )
        {
            this.category = category;
            return this;
        }

        /**
         * Use the specified log {@link Level} as a default.
         *
         * @param level the log level to use as a default
         * @return this builder
         */
        public Builder withLogLevel( Level level )
        {
            this.level = level;
            return this;
        }

        /**
         * Disable auto flushing.
         *
         * @return this builder
         */
        public Builder withoutAutoFlush()
        {
            autoFlush = false;
            return this;
        }

        /**
         * Creates a {@link BrokerLog} instance that writes messages to an {@link OutputStream}.
         *
         * @param out An {@link OutputStream} to write to
         * @return A {@link BrokerLog} instance that writes to the specified OutputStream
         */
        public BrokerLog toOutputStream( OutputStream out )
        {
            return toPrintWriter( Suppliers.singleton( OUTPUT_STREAM_CONVERTER.apply( out ) ) );
        }

        /**
         * Creates a {@link BrokerLog} instance that writes messages to {@link OutputStream}s obtained from the specified
         * {@link Supplier}. The OutputStream is obtained from the Supplier before every log message is written.
         *
         * @param outSupplier A supplier for an output stream to write to
         * @return A {@link BrokerLog} instance
         */
        public BrokerLog toOutputStream( Supplier<OutputStream> outSupplier )
        {
            return toPrintWriter( Suppliers.adapted( outSupplier, OUTPUT_STREAM_CONVERTER ) );
        }

        /**
         * Creates a {@link BrokerLog} instance that writes messages to a {@link Writer}.
         *
         * @param writer A {@link Writer} to write to
         * @return A {@link BrokerLog} instance that writes to the specified Writer
         */
        public BrokerLog toWriter( Writer writer )
        {
            return toPrintWriter( new PrintWriter( writer ) );
        }

        /**
         * Creates a {@link BrokerLog} instance that writes messages to a {@link PrintWriter}.
         *
         * @param writer A {@link PrintWriter} to write to
         * @return A {@link BrokerLog} instance that writes to the specified PrintWriter
         */
        public BrokerLog toPrintWriter( PrintWriter writer )
        {
            return toPrintWriter( Suppliers.singleton( writer ) );
        }

        /**
         * Creates a {@link BrokerLog} instance that writes messages to {@link PrintWriter}s obtained from the specified
         * {@link Supplier}. The PrintWriter is obtained from the Supplier before every log message is written.
         *
         * @param writerSupplier A supplier for a {@link PrintWriter} to write to
         * @return A {@link BrokerLog} instance that writes to the specified PrintWriter
         */
        public BrokerLog toPrintWriter( Supplier<PrintWriter> writerSupplier )
        {
            return new BrokerLog( DEFAULT_CURRENT_DATE_SUPPLIER, writerSupplier, timezone, lock, category, level, autoFlush );
        }
    }

    private final Supplier<Date> currentDateSupplier;
    private final Supplier<PrintWriter> writerSupplier;
    private final TimeZone timezone;
    private final Object lock;
    private final String category;
    private final AtomicReference<Level> levelRef;
    private final boolean autoFlush;
    private final Logger debugLogger;
    private final Logger infoLogger;
    private final Logger warnLogger;
    private final Logger errorLogger;

    /**
     * Start creating a {@link BrokerLog} with UTC timezone for datestamps in the log
     *
     * @return a builder for a {@link BrokerLog}
     */
    public static Builder withUTCTimeZone()
    {
        return new Builder().withUTCTimeZone();
    }

    /**
     * Start creating a {@link BrokerLog} with the specified timezone for datestamps in the log
     *
     * @param timezone the timezone to use for datestamps
     * @return a builder for a {@link BrokerLog}
     */
    public static Builder withTimeZone( TimeZone timezone )
    {
        return new Builder().withTimeZone( timezone );
    }

    /**
     * Start creating a {@link BrokerLog} using the specified object to synchronize on.
     * Use {@link Builder#toOutputStream} to complete.
     *
     * @param lock the object to synchronize on
     * @return a builder for a {@link BrokerLog}
     */
    public static Builder usingLock( Object lock )
    {
        return new Builder().usingLock( lock );
    }

    /**
     * Include the specified category in each output log line
     *
     * @param category the category to include ing each output line
     * @return a builder for a {@link BrokerLog}
     */
    public static Builder withCategory( String category )
    {
        return new Builder().withCategory( category );
    }

    /**
     * Start creating a {@link BrokerLog} with the specified log {@link Level} as a default.
     * Use {@link Builder#toOutputStream} to complete.
     *
     * @param level the log level to use as a default
     * @return a builder for a {@link BrokerLog}
     */
    public static Builder withLogLevel( Level level )
    {
        return new Builder().withLogLevel( level );
    }

    /**
     * Start creating a {@link BrokerLog} without auto flushing.
     * Use {@link Builder#toOutputStream} to complete.
     *
     * @return a builder for a {@link BrokerLog}
     */
    public static Builder withoutAutoFlush()
    {
        return new Builder().withoutAutoFlush();
    }

    /**
     * Creates a {@link BrokerLog} instance that writes messages to an {@link OutputStream}.
     *
     * @param out An {@link OutputStream} to write to
     * @return A {@link BrokerLog} instance that writes to the specified OutputStream
     */
    public static BrokerLog toOutputStream( OutputStream out )
    {
        return new Builder().toOutputStream( out );
    }

    /**
     * Creates a {@link BrokerLog} instance that writes messages to {@link OutputStream}s obtained from the specified
     * {@link Supplier}. The OutputStream is obtained from the Supplier before every log message is written.
     *
     * @param outSupplier A supplier for an output stream to write to
     * @return A {@link BrokerLog} instance
     */
    public static BrokerLog toOutputStream( Supplier<OutputStream> outSupplier )
    {
        return new Builder().toOutputStream( outSupplier );
    }

    /**
     * Creates a {@link BrokerLog} instance that writes messages to a {@link Writer}.
     *
     * @param writer A {@link Writer} to write to
     * @return A {@link BrokerLog} instance that writes to the specified Writer
     */
    public static BrokerLog toWriter( Writer writer )
    {
        return new Builder().toWriter( writer );
    }

    /**
     * Creates a {@link BrokerLog} instance that writes messages to a {@link PrintWriter}.
     *
     * @param writer A {@link PrintWriter} to write to
     * @return A {@link BrokerLog} instance that writes to the specified PrintWriter
     */
    public static BrokerLog toPrintWriter( PrintWriter writer )
    {
        return new Builder().toPrintWriter( writer );
    }

    /**
     * Creates a {@link BrokerLog} instance that writes messages to {@link PrintWriter}s obtained from the specified
     * {@link Supplier}. The PrintWriter is obtained from the Supplier before every log message is written.
     *
     * @param writerSupplier A supplier for a {@link PrintWriter} to write to
     * @return A {@link BrokerLog} instance that writes to the specified PrintWriter
     */
    public static BrokerLog toPrintWriter( Supplier<PrintWriter> writerSupplier )
    {
        return new Builder().toPrintWriter( writerSupplier );
    }

    protected BrokerLog( Supplier<Date> currentDateSupplier, Supplier<PrintWriter> writerSupplier, TimeZone timezone, Object maybeLock, String category,
            Level level, boolean autoFlush )
    {
        this.currentDateSupplier = currentDateSupplier;
        this.writerSupplier = writerSupplier;
        this.timezone = timezone;
        this.lock = (maybeLock != null) ? maybeLock : this;
        this.category = category;
        this.levelRef = new AtomicReference<>( level );
        this.autoFlush = autoFlush;

        String debugPrefix = (category != null && !category.isEmpty()) ? "DEBUG [" + category + "]" : "DEBUG";
        String infoPrefix = (category != null && !category.isEmpty()) ? "INFO [" + category + "]" : "INFO ";
        String warnPrefix = (category != null && !category.isEmpty()) ? "WARN [" + category + "]" : "WARN ";
        String errorPrefix = (category != null && !category.isEmpty()) ? "ERROR [" + category + "]" : "ERROR";

        this.debugLogger = new BrokerWriterLogger( writerSupplier, debugPrefix );
        this.infoLogger = new BrokerWriterLogger( writerSupplier, infoPrefix );
        this.warnLogger = new BrokerWriterLogger( writerSupplier, warnPrefix );
        this.errorLogger = new BrokerWriterLogger( writerSupplier, errorPrefix );
    }

    /**
     * Get the current {@link Level} that logging is enabled at
     *
     * @return the current level that logging is enabled at
     */
    public Level getLevel()
    {
        return levelRef.get();
    }

    /**
     * Set the {@link Level} that logging should be enabled at
     *
     * @param level the new logging level
     * @return the previous logging level
     */
    public Level setLevel( Level level )
    {
        return levelRef.getAndSet( level );
    }

    @Override
    public boolean isDebugEnabled()
    {
        return Level.DEBUG.compareTo( levelRef.get() ) >= 0;
    }

    @Nonnull
    @Override
    public Logger debugLogger()
    {
        return isDebugEnabled() ? this.debugLogger : NullLogger.getInstance();
    }

    /**
     * @return true if the current log level enables info logging
     */
    public boolean isInfoEnabled()
    {
        return Level.INFO.compareTo( levelRef.get() ) >= 0;
    }

    @Nonnull
    @Override
    public Logger infoLogger()
    {
        return isInfoEnabled() ? this.infoLogger : NullLogger.getInstance();
    }

    /**
     * @return true if the current log level enables warn logging
     */
    public boolean isWarnEnabled()
    {
        return Level.WARN.compareTo( levelRef.get() ) >= 0;
    }

    @Nonnull
    @Override
    public Logger warnLogger()
    {
        return isWarnEnabled() ? this.warnLogger : NullLogger.getInstance();
    }

    /**
     * @return true if the current log level enables error logging
     */
    public boolean isErrorEnabled()
    {
        return Level.ERROR.compareTo( levelRef.get() ) >= 0;
    }

    @Nonnull
    @Override
    public Logger errorLogger()
    {
        return isErrorEnabled() ? this.errorLogger : NullLogger.getInstance();
    }

    @Override
    public void bulk( @Nonnull Consumer<Log> consumer )
    {
        PrintWriter writer;
        synchronized ( lock )
        {
            writer = writerSupplier.get();
            consumer.accept( new BrokerLog( currentDateSupplier, Suppliers.singleton( writer ), timezone, lock, category, levelRef.get(), false ) );
        }
        if ( autoFlush )
        {
            writer.flush();
        }
    }

    private class BrokerWriterLogger extends AbstractPrintWriterLogger
    {
        private final String prefix;
        private final DateFormat format;

        BrokerWriterLogger( @Nonnull Supplier<PrintWriter> writerSupplier, @Nonnull String prefix )
        {
            super( writerSupplier, lock, autoFlush );
            this.prefix = prefix;
            format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSSZ" );
            format.setTimeZone( timezone );
        }

        @Override
        protected void writeLog( @Nonnull PrintWriter out, @Nonnull String message )
        {
            lineStart( out );
            out.write( message );
            out.println();
        }

        @Override
        protected void writeLog( @Nonnull PrintWriter out, @Nonnull String message, @Nonnull Throwable throwable )
        {
            lineStart( out );
            out.write( message );
            if ( throwable.getMessage() != null )
            {
                out.write( ' ' );
                out.write( throwable.getMessage() );
            }
            out.println();
            throwable.printStackTrace( out );
        }

        @Override
        protected Logger getBulkLogger( @Nonnull PrintWriter out, @Nonnull Object lock )
        {
            return new BrokerWriterLogger( Suppliers.singleton( out ), prefix );
        }

        private void lineStart( PrintWriter out )
        {
            out.write( time() );
            out.write( ' ' );
            out.write( prefix );
            out.write( ' ' );
        }

        private String time()
        {
            return format.format( currentDateSupplier.get() );
        }
    }
}