package org.apache.avro.file;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * 在处理 Avro Input Stream 时，对于 Bytes 类型的字段只加载前 1024 字节到内存中
 * 用于文件类型检测扫描场景，防止大视频文件全量加载到内存导致的 OOM
 *
 * @param <D>
 */
public class MagicHeaderDatumReader<D> extends GenericDatumReader<D> {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public D read(D reuse, Decoder in) throws IOException {
        Class<?> clazz = GenericDatumReader.class;
        Schema actual = null;
        Schema expected = null;
        try {
            Field actualField = clazz.getDeclaredField("actual");
            actualField.setAccessible(true);
            actual = (Schema) actualField.get(this);
            Field expectedField = clazz.getDeclaredField("expected");
            expectedField.setAccessible(true);
            expected = (Schema) expectedField.get(this);
        } catch (Throwable e) {
            e.printStackTrace();
            logger.error("read", e);
        }
        ResolvingDecoder resolver = getResolver(actual, expected);
        BinaryDecoder magicHeaderProxyDecoder = (BinaryDecoder) new MagicHeaderBinaryDecoderCglibProxy(in).getProxyInstance();
        resolver.configure(magicHeaderProxyDecoder);
        D result = (D) read(reuse, expected, resolver);
        resolver.drain();
        return result;
    }

}
