package com.senzing.datamart.reports;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;

import com.senzing.sql.ConnectionProvider;

/**
 * A test utility {@link ConnectionProvider} implementation that returns proxy
 * {@link Connection} instances which throw a specified exception type for all
 * method calls except {@link Connection#close()}.
 *
 * <p>This class is designed for testing error handling paths in code that uses
 * {@link ConnectionProvider} and {@link Connection} objects. The returned
 * connections will throw the configured exception type (with message "Simulated Failure")
 * for any method invocation except {@code close()}, which safely returns without
 * side effects.</p>
 *
 * <p><b>Example Usage:</b></p>
 * <pre>
 * // Create provider that throws SQLException on all Connection method calls
 * ConnectionProvider provider = new ThrowingConnectionProvider(SQLException.class);
 *
 * // Get a connection - this succeeds
 * Connection conn = provider.getConnection();
 *
 * // Any method except close() will throw SQLException
 * conn.prepareStatement("SELECT * FROM table"); // throws SQLException
 *
 * // close() succeeds without throwing
 * conn.close(); // OK
 * </pre>
 *
 * <p><b>Requirements:</b> The exception type must have a constructor that
 * accepts a single {@link String} parameter, otherwise construction will fail
 * with {@link IllegalArgumentException}.</p>
 */
public class ThrowingConnectionProvider implements ConnectionProvider {
    /**
     * The {@link Connection#close()} method, cached for performance.
     */
    private static final Method CLOSE_METHOD;

    /**
     * The error message used when throwing simulated exceptions.
     */
    private static final String SIMULATED_FAILURE_MESSAGE = "Simulated Failure";

    static {
        try {
            CLOSE_METHOD = Connection.class.getMethod("close");
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Internal {@link InvocationHandler} that implements the proxy behavior
     * for {@link Connection} objects returned by this provider.
     *
     * <p>All method invocations except {@link Connection#close()} will throw
     * a new instance of the configured exception type.</p>
     */
    protected class Handler implements InvocationHandler {
        /**
         * Handles method invocations on the proxy {@link Connection}.
         *
         * @param proxy The proxy instance.
         * @param method The method being invoked.
         * @param args The method arguments.
         * @return {@code null} for {@code close()}, otherwise this method throws.
         * @throws Throwable A new instance of the configured exception type.
         */
        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable
        {
            // Allow close() to succeed without side effects
            if (method.equals(CLOSE_METHOD)) {
                return null;
            }

            // All other methods throw the configured exception
            ThrowingConnectionProvider provider = ThrowingConnectionProvider.this;
            throw provider.constructor.newInstance(SIMULATED_FAILURE_MESSAGE);
        }
    }

    /**
     * The type of exception to throw from {@link Connection} method calls.
     */
    private Class<? extends Throwable> throwType = null;

    /**
     * The constructor for creating exception instances.
     */
    private Constructor<? extends Throwable> constructor = null;

    /**
     * The invocation handler used for proxy connections.
     */
    private Handler handler = null;

    /**
     * Constructs a new {@link ThrowingConnectionProvider} that will return
     * {@link Connection} proxies which throw the specified exception type.
     *
     * @param throwType The exception class to throw from {@link Connection}
     *                  method calls. Must have a constructor accepting a single
     *                  {@link String} parameter.
     * @throws IllegalArgumentException If the specified exception type does not
     *                                  have a constructor that accepts a single
     *                                  {@link String} parameter.
     */
    public ThrowingConnectionProvider(Class<? extends Throwable> throwType)
    {
        this.throwType = throwType;
        try {
            this.constructor = throwType.getConstructor(String.class);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Exception type must have a constructor accepting a String parameter: "
                + throwType.getName(), e);
        }
        this.handler = new Handler();
    }

    /**
     * Gets the {@link Class} for the type of exception that is thrown by 
     * {@link Connection} instances created by this instance.
     * 
     * @return The {@link Class} for the type of exception that is thrown by 
     *         {@link Connection} instances created by this instance.
     */
    public Class<? extends Throwable> getThrowType() {
        return this.throwType;
    }

    /**
     * Returns a proxy {@link Connection} that throws the configured exception
     * type for all method calls except {@link Connection#close()}.
     *
     * <p>The returned connection is a dynamic proxy that intercepts all method
     * calls. The {@code close()} method returns normally, but all other methods
     * will throw a new instance of the exception type specified in the constructor.</p>
     *
     * @return A proxy {@link Connection} that simulates failures.
     * @throws SQLException This implementation never actually throws, but the
     *                      signature is required by {@link ConnectionProvider}.
     */
    @Override
    public Connection getConnection() throws SQLException {
        Class<?>[] interfaces = { Connection.class };
        return (Connection) Proxy.newProxyInstance(
            this.getClass().getClassLoader(), interfaces, this.handler);
    }

}
