package org.apache.cockpit.connectors.api.jdbc.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.lang.reflect.*;
import java.sql.Connection;
import java.sql.SQLException;

class CopyManagerProxy {
    private static final Logger LOG = LoggerFactory.getLogger(CopyManagerProxy.class);
    Object connection;
    Object copyManager;
    Class<?> connectionClazz;
    Class<?> copyManagerClazz;
    Method getCopyAPIMethod;
    Method copyInMethod;

    CopyManagerProxy(Connection connection)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
                    SQLException {
        LOG.info("Proxy connection class: {}", connection.getClass().getName());
        this.connection = connection.unwrap(Connection.class);
        LOG.info("Proxy unwrap connection class: {}", this.connection.getClass().getName());
        if (Proxy.isProxyClass(this.connection.getClass())) {
            InvocationHandler handler = Proxy.getInvocationHandler(this.connection);
            this.connection = getConnectionFromInvocationHandler(handler);
            if (null == this.connection) {
                throw new InvocationTargetException(
                        new NullPointerException("Proxy Connection is null."));
            }
            LOG.info("Proxy connection class: {}", this.connection.getClass().getName());
            this.connectionClazz = this.connection.getClass();
        } else {
            this.connectionClazz = this.connection.getClass();
        }
        this.getCopyAPIMethod = this.connectionClazz.getMethod("getCopyAPI");
        this.copyManager = this.getCopyAPIMethod.invoke(this.connection);
        this.copyManagerClazz = this.copyManager.getClass();
        this.copyInMethod = this.copyManagerClazz.getMethod("copyIn", String.class, Reader.class);
    }

    long doCopy(String sql, Reader reader)
            throws InvocationTargetException, IllegalAccessException {
        return (long) this.copyInMethod.invoke(this.copyManager, sql, reader);
    }

    private static Object getConnectionFromInvocationHandler(InvocationHandler handler)
            throws IllegalAccessException {
        Class<?> handlerClass = handler.getClass();
        LOG.info("InvocationHandler class: {}", handlerClass.getName());
        for (Field declaredField : handlerClass.getDeclaredFields()) {
            boolean tempAccessible = declaredField.isAccessible();
            if (!tempAccessible) {
                declaredField.setAccessible(true);
            }
            Object handlerObject = declaredField.get(handler);
            if (handlerObject instanceof Connection) {
                if (!tempAccessible) {
                    declaredField.setAccessible(tempAccessible);
                }
                return handlerObject;
            } else {
                if (!tempAccessible) {
                    declaredField.setAccessible(tempAccessible);
                }
            }
        }
        return null;
    }
}
