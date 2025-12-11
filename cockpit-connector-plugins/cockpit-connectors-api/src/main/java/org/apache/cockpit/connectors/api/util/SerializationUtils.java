package org.apache.cockpit.connectors.api.util;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

import java.io.*;

public class SerializationUtils {

    public static String objectToString(Serializable obj) {
        if (obj != null) {
            return Base64.encodeBase64String(serialize(obj));
        }
        return null;
    }

    public static <T extends Serializable> T stringToObject(String str) {
        if (StringUtils.isNotEmpty(str)) {
            return deserialize(Base64.decodeBase64(str));
        }
        return null;
    }

    public static <T extends Serializable> byte[] serialize(T obj) {
        try (ByteArrayOutputStream b = new ByteArrayOutputStream(512);
                ObjectOutputStream out = new ObjectOutputStream(b)) {
            out.writeObject(obj);
            return b.toByteArray();
        } catch (final IOException ex) {
            throw new SerializationException(ex);
        }
    }

    public static <T extends Serializable> T deserialize(byte[] bytes) {
        try (ByteArrayInputStream s = new ByteArrayInputStream(bytes);
                ObjectInputStream in =
                        new ObjectInputStream(s) {
                            @Override
                            protected Class<?> resolveClass(ObjectStreamClass desc)
                                    throws IOException, ClassNotFoundException {
                                // make sure use current thread classloader
                                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                                if (cl == null) {
                                    return super.resolveClass(desc);
                                }
                                return Class.forName(desc.getName(), false, cl);
                            }
                        }) {
            @SuppressWarnings("unchecked")
            final T obj = (T) in.readObject();
            return obj;
        } catch (final ClassNotFoundException | IOException ex) {
            throw new SerializationException(ex);
        }
    }

    public static <T extends Serializable> T deserialize(byte[] bytes, ClassLoader classLoader) {
        try (ByteArrayInputStream s = new ByteArrayInputStream(bytes);
                ObjectInputStream in =
                        new ObjectInputStream(s) {
                            @Override
                            protected Class<?> resolveClass(ObjectStreamClass desc)
                                    throws IOException, ClassNotFoundException {
                                // make sure use current thread classloader
                                if (classLoader == null) {
                                    return super.resolveClass(desc);
                                }
                                return Class.forName(desc.getName(), false, classLoader);
                            }
                        }) {
            @SuppressWarnings("unchecked")
            final T obj = (T) in.readObject();
            return obj;
        } catch (final ClassNotFoundException | IOException ex) {
            throw new SerializationException(ex);
        }
    }
}
