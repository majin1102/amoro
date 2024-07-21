/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.utils;

import static org.apache.amoro.shade.guava32.com.google.common.base.Preconditions.checkNotNull;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;

@SuppressWarnings({"unchecked", "rawtypes"})
public class KryoSerializationUtil {

  private static final ThreadLocal<KryoSerializerInstance> KRYO_SERIALIZER =
      ThreadLocal.withInitial(KryoSerializerInstance::new);

  public static byte[] kryoSerialize(final Object obj) {
    return KRYO_SERIALIZER.get().serialize(obj);
  }

  @SuppressWarnings("unchecked")
  public static <T> T kryoDeserialize(final byte[] objectData) {
    if (objectData == null) {
      throw new NullPointerException("The byte[] must not be null");
    }
    return (T) KRYO_SERIALIZER.get().deserialize(objectData);
  }

  public static <T> SimpleSerializer<T> createJavaSimpleSerializer() {
    return JavaSerializer.INSTANT;
  }

  public static SimpleSerializer<StructLikeWrapper> createStructLikeWrapperSerializer(
      StructLikeWrapper structLikeWrapper) {
    return new StructLikeWrapperSerializer(structLikeWrapper);
  }

  private static class KryoSerializerInstance implements Serializable {
    public static final int KRYO_SERIALIZER_INITIAL_BUFFER_SIZE = 1048576;
    private final Kryo kryo;
    private final ByteArrayOutputStream outputStream;

    KryoSerializerInstance() {
      KryoInstantiator kryoInstantiator = new KryoInstantiator();
      kryo = kryoInstantiator.newKryo();
      outputStream = new ByteArrayOutputStream(KRYO_SERIALIZER_INITIAL_BUFFER_SIZE);
      kryo.setRegistrationRequired(false);
    }

    byte[] serialize(Object obj) {
      kryo.reset();
      outputStream.reset();
      Output output = new Output(outputStream);
      this.kryo.writeClassAndObject(output, obj);
      output.close();
      return outputStream.toByteArray();
    }

    Object deserialize(byte[] objectData) {
      return this.kryo.readClassAndObject(new Input(objectData));
    }
  }

  private static class KryoInstantiator implements Serializable {

    public Kryo newKryo() {
      Kryo kryo = new Kryo();

      // This instance of Kryo should not require prior registration of classes
      kryo.setRegistrationRequired(false);
      Kryo.DefaultInstantiatorStrategy instantiatorStrategy =
          new Kryo.DefaultInstantiatorStrategy();
      instantiatorStrategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
      kryo.setInstantiatorStrategy(instantiatorStrategy);
      // Handle cases where we may have an odd classloader setup like with lib jars
      // for hadoop
      kryo.setClassLoader(Thread.currentThread().getContextClassLoader());

      // Register serializers
      kryo.register(Utf8.class, new AvroUtf8Serializer());

      return kryo;
    }
  }

  private static class AvroUtf8Serializer extends Serializer<Utf8> {

    @SuppressWarnings("unchecked")
    @Override
    public void write(Kryo kryo, Output output, Utf8 utf8String) {
      Serializer<byte[]> bytesSerializer = kryo.getDefaultSerializer(byte[].class);
      bytesSerializer.write(kryo, output, utf8String.getBytes());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Utf8 read(Kryo kryo, Input input, Class<Utf8> type) {
      Serializer<byte[]> bytesSerializer = kryo.getDefaultSerializer(byte[].class);
      byte[] bytes = bytesSerializer.read(kryo, input, byte[].class);
      return new Utf8(bytes);
    }
  }

  public interface SimpleSerializer<T> {

    byte[] serialize(T t);

    T deserialize(byte[] bytes);
  }

  public static class StructLikeWrapperSerializer implements SimpleSerializer<StructLikeWrapper> {

    protected final StructLikeWrapper structLikeWrapper;

    public StructLikeWrapperSerializer(StructLikeWrapper structLikeWrapper) {
      this.structLikeWrapper = structLikeWrapper;
    }

    public StructLikeWrapperSerializer(Types.StructType type) {
      this.structLikeWrapper = StructLikeWrapper.forType(type);
    }

    @Override
    public byte[] serialize(StructLikeWrapper structLikeWrapper) {
      checkNotNull(structLikeWrapper);
      StructLike copy = KryoSerializationUtil.StructLikeCopy.copy(structLikeWrapper.get());
      return KryoSerializationUtil.kryoSerialize(copy);
    }

    @Override
    public StructLikeWrapper deserialize(byte[] bytes) {
      if (bytes == null) {
        return null;
      }
      KryoSerializationUtil.StructLikeCopy structLike =
          KryoSerializationUtil.kryoDeserialize(bytes);
      return structLikeWrapper.copyFor(structLike);
    }
  }

  public static class JavaSerializer<T extends Serializable> implements SimpleSerializer<T> {

    public static final JavaSerializer INSTANT = new JavaSerializer<>();

    @Override
    public byte[] serialize(T t) {
      checkNotNull(t);
      return KryoSerializationUtil.kryoSerialize(t);
    }

    @Override
    public T deserialize(byte[] bytes) {
      if (bytes == null) {
        return null;
      }
      return KryoSerializationUtil.kryoDeserialize(bytes);
    }
  }

  public static class StructLikeCopy implements StructLike {

    public static StructLike copy(StructLike struct) {
      return struct != null ? new StructLikeCopy(struct) : null;
    }

    private final Object[] values;

    private StructLikeCopy(StructLike toCopy) {
      this.values = new Object[toCopy.size()];

      for (int i = 0; i < values.length; i += 1) {
        Object value = toCopy.get(i, Object.class);

        if (value instanceof StructLike) {
          values[i] = copy((StructLike) value);
        } else {
          values[i] = value;
        }
      }
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Struct copy cannot be modified");
    }
  }
}
