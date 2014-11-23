package com.twitter.elephantbird.mapreduce.io;

import com.twitter.elephantbird.thrift.ThriftBinaryDeserializer;
import com.twitter.elephantbird.thrift.ThriftCompactDeserializer;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.util.TypeRef;

public class ThriftConverter<M extends TBase<?, ?>> implements BinaryConverter<M> {

  public static final Logger LOG = LoggerFactory.getLogger(ThriftConverter.class);

  private TypeRef<M> typeRef;
  private TSerializer serializer;
  private TDeserializer deserializer;

  // limit the number of warnings in case of serialization errors.
  private static final int MAX_WARNINGS = 100;
  private static int numWarningsLogged = 0;

  private static void logWarning(String message, Throwable t) {
    // does not need to be thread safe
    if ( numWarningsLogged < MAX_WARNINGS ) {
      LOG.info(message, t);
      numWarningsLogged++;
    }
  }

  public enum ThriftProtocols {
    TCOMPACTPROTOCOL {
      @Override
      public String toString() {
        return "TCOMPACTPROTOCOL";
      }
    } ,

    TBINARYPROTOCOL {
      @Override
      public String toString() {
        return "TBINARYPROTOCOL";
      }
    }
  }

  private ThriftProtocols protocol = null;

  /**
   * Returns a ThriftConverter for a given Thrift class.
   */
  public static <M extends TBase<?, ?>> ThriftConverter<M> newInstance(Class<M> tClass) {
    return new ThriftConverter<M>(new TypeRef<M>(tClass){});
  }

  public static <M extends TBase<?, ?>> ThriftConverter<M> newInstance(TypeRef<M> typeRef) {
    return new ThriftConverter<M>(typeRef);
  }

  public static <M extends TBase<?, ?>> ThriftConverter<M> newInstance(Class<M> tClass, ThriftProtocols thriftProtocols) {
    return new ThriftConverter<M>(new TypeRef<M>(tClass){}, thriftProtocols);
  }

  public static <M extends TBase<?, ?>> ThriftConverter<M> newInstance(TypeRef<M> typeRef, ThriftProtocols thriftProtocols) {
    return new ThriftConverter<M>(typeRef, thriftProtocols);
  }

  public ThriftConverter(TypeRef<M> typeRef) {
    this(typeRef, ThriftProtocols.TBINARYPROTOCOL);
  }

  public ThriftConverter(TypeRef<M> typeRef, ThriftProtocols thriftProtocols) {
    this.typeRef = typeRef;

    switch (thriftProtocols) {
      case TBINARYPROTOCOL:
        deserializer = new ThriftBinaryDeserializer();
        break;
      case TCOMPACTPROTOCOL:
        deserializer = new ThriftCompactDeserializer();
        break;
      default:
        throw new UnsupportedOperationException("Protocol " + thriftProtocols + " not yet supported");
    }
  }

  @Override
  public M fromBytes(byte[] messageBuffer) {
    try {
      M message = typeRef.safeNewInstance();
      deserializer.deserialize(message, messageBuffer);
      return message;
    } catch (Throwable e) {
      // normally a TException. but some corrupt records can cause
      // other runtime exceptions (e.g. IndexOutOfBoundsException).
      logWarning("failed to deserialize", e);
      return null;
    }
  }

  @Override
  public byte[] toBytes(M message) {
    if (serializer == null) {
      if (deserializer instanceof ThriftCompactDeserializer) {
        serializer = new TSerializer(new TCompactProtocol.Factory());
      } else {
        serializer = new TSerializer();
      }
    }

    try {
      return serializer.serialize(message);
    } catch (TException e) {
      logWarning("failed to serialize", e);
      return null;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    try {
      return typeRef.getType().equals(((ThriftConverter<?>)obj).typeRef.getType());
    } catch (ClassCastException e) {
      return false;
    }
  }
}
