package org.apache.avro.file;


import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * 在读取 Avro 读取 Bytes 的时候只读取前 1024 个字节，以防止 OOM
 * {@link org.apache.avro.io.BinaryDecoder} 的代理类
 */
public class MagicHeaderBinaryDecoderCglibProxy implements MethodInterceptor {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private Object target;
    Class<?> targetClazz;

    public MagicHeaderBinaryDecoderCglibProxy(Object target) {
        this.target = target;
        this.targetClazz = target.getClass();
    }

    /**
     * 获取代理对象
     *
     * @return
     */
    public Object getProxyInstance() {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(targetClazz);
        enhancer.setCallback(this);
        logger.info("targetClazz: " + targetClazz + ", targetClazz.getInterfaces().length: " + targetClazz.getInterfaces().length);
        for (Class<?> anInterface : targetClazz.getInterfaces()) {
            logger.info("targetClazz: " + targetClazz + ",targetClazz.getInterfaces(): " + anInterface);
        }
        return enhancer.create();
    }

    /**
     * 只读取 bytes 流的文件魔数，而不是全部的数据，因为有的 Avro Record 存放的是
     * 一个好几G的视频文件，在并发扫描文件类型时非常容易出现 OOM
     *
     * @param targetClazz
     * @param args
     * @return
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    public ByteBuffer readMagicHeaderBytes(Class<?> targetClazz, Object[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        ByteBuffer old = (ByteBuffer) args[0];
        int bytesLength = (int) targetClazz.getMethod("readInt").invoke(target);
        int magicHeaderLength = 1024;
        ByteBuffer result;
        if (old != null && bytesLength <= old.capacity()) {
            result = old;
            result.clear();
        } else {
            result = ByteBuffer.allocate(magicHeaderLength);
        }
        logger.info("doReadBytes(result.array(), result.position(), magicHeaderLength)");
        Method doReadBytesMethod = targetClazz.getDeclaredMethod("doReadBytes", byte[].class, int.class, int.class);
        doReadBytesMethod.setAccessible(true);
        doReadBytesMethod.invoke(target, result.array(), result.position(), magicHeaderLength);
        result.limit(magicHeaderLength);
        long remainingSkip = bytesLength - magicHeaderLength;
        logger.info("doSkipBytes(" + remainingSkip + ")");
        Method doSkipBytesMethod = targetClazz.getDeclaredMethod("doSkipBytes", long.class);
        doSkipBytesMethod.setAccessible(true);
        doSkipBytesMethod.invoke(target, remainingSkip);
        return result;
    }

    @Override
    public Object intercept(Object o, Method method, Object[] args, MethodProxy methodProxy) throws Throwable {
        Method readBytesMethod = targetClazz.getMethod("readBytes", ByteBuffer.class);
        if (method.equals(readBytesMethod)) {
            return readMagicHeaderBytes(targetClazz, args);
        } else {
            return method.invoke(target, args);
        }
    }
}
