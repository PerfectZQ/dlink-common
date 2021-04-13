package org.apache.avro.file;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * {@link DataFileStream}
 */
public class MagicHeaderDataFileReader<D> extends DataFileReader<D> {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private long actualBlockSize;
    private long blockSize;

    /**
     * Construct a reader for a file.
     */
    public MagicHeaderDataFileReader(SeekableInput sin, DatumReader<D> reader) throws IOException {
        super(sin, reader, false);
    }

    @Override
    public boolean hasNext() {
        return super.hasNext();
    }

    @Override
    boolean hasNextBlock() {
        logger.info("====== hasNextBlock invoked");
        try {
            boolean availableBlock = (boolean) getFieldValue(DataFileStream.class, this, "availableBlock");
            if (availableBlock)
                return true;
            if (vin.isEnd())
                return false;
            blockRemaining = vin.readLong(); // read block count
            actualBlockSize = vin.readLong();
            if (actualBlockSize > Integer.MAX_VALUE || actualBlockSize < 0) {
                throw new IOException("Block size invalid or too large for this " + "implementation: " + blockSize);
            }
            long sizeThreshold = 10 * 1024 * 1024;
            blockSize = Math.min(actualBlockSize, sizeThreshold);
            setFieldValue(DataFileStream.class, this, "blockSize", blockSize);
            blockCount = blockRemaining;
            // availableBlock = true;
            setFieldValue(DataFileStream.class, this, "availableBlock", true);
            return true;
        } catch (EOFException eof) {
            return false;
        } catch (IOException | NoSuchFieldException | IllegalAccessException e) {
            throw new AvroRuntimeException(e);
        }
    }

    @Override
    DataBlock nextRawBlock(DataBlock reuse) throws IOException {
        logger.info("====== nextRawBlock(DataBlock reuse) invoked");
        if (!hasNextBlock()) {
            throw new NoSuchElementException();
        }
        try {
            blockSize = (long) getFieldValue(DataFileStream.class, this, "blockSize");
            Constructor<DataBlock> constructor = DataBlock.class.getDeclaredConstructor(long.class, int.class);
            constructor.setAccessible(true);
            if (reuse == null) {
                reuse = constructor.newInstance(blockRemaining, (int) blockSize);
            } else {
                byte[] reuseData = (byte[]) getFieldValue(DataBlock.class, reuse, "data");
                if (reuseData.length < (int) blockSize) {
                    reuse = constructor.newInstance(blockRemaining, (int) blockSize);
                } else {
                    // reuse.numEntries = blockRemaining;
                    setFieldValue(DataBlock.class, reuse, "numEntries", blockRemaining);
                    // reuse.blockSize = (int) blockSize;
                    setFieldValue(DataBlock.class, reuse, "blockSize", (int) blockSize);
                }
            }
            byte[] reuseData = (byte[]) getFieldValue(DataBlock.class, reuse, "data");
            int reuseBlockSize = (int) getFieldValue(DataBlock.class, reuse, "blockSize");
            // throws if it can't read the size requested
            vin.readFixed(reuseData, 0, reuseBlockSize);
            long skipSize = actualBlockSize - reuseBlockSize;
            if (skipSize > 0) {
                vin.skipFixed((int) skipSize);
                logger.info("vin.skipFixed(" + (int) skipSize + ")");
            }
            vin.readFixed(syncBuffer);
            // availableBlock = false;
            setFieldValue(DataFileStream.class, this, "availableBlock", false);
            if (!Arrays.equals(syncBuffer, getHeader().sync))
                throw new IOException("Invalid sync!");
        } catch (NoSuchFieldException | IllegalAccessException | NoSuchMethodException | InstantiationException | InvocationTargetException e) {
            e.printStackTrace();
            logger.error("nextRawBlock", e);
        }
        return reuse;
    }

    public void setFieldValue(Class<?> clazz, Object target, String fieldName, Object value) throws NoSuchFieldException, IllegalAccessException {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    public Object getFieldValue(Class<?> clazz, Object target, String fieldName) throws NoSuchFieldException, IllegalAccessException {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}
