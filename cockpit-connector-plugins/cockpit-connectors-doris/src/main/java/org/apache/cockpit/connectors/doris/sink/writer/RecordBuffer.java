package org.apache.cockpit.connectors.doris.sink.writer;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.cockpit.connectors.doris.exception.DorisConnectorException;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

/** Channel of record stream and HTTP data stream. */
@Slf4j
public class RecordBuffer {
    private final BlockingQueue<ByteBuffer> writeQueue;
    private final BlockingQueue<ByteBuffer> readQueue;
    private final int bufferCapacity;
    private final int queueSize;
    private ByteBuffer currentWriteBuffer;
    private ByteBuffer currentReadBuffer;
    // used to check stream load error by stream load thread
    @Setter private volatile String errorMessageByStreamLoad;

    public RecordBuffer(int capacity, int queueSize) {
        log.info("init RecordBuffer capacity {}, count {}", capacity, queueSize);
        checkState(capacity > 0);
        checkState(queueSize > 1);
        this.writeQueue = new ArrayBlockingQueue<>(queueSize);
        for (int index = 0; index < queueSize; index++) {
            this.writeQueue.add(ByteBuffer.allocate(capacity));
        }
        readQueue = new LinkedBlockingDeque<>();
        this.bufferCapacity = capacity;
        this.queueSize = queueSize;
    }

    public void startBufferData() {
        log.info(
                "start buffer data, read queue size {}, write queue size {}",
                readQueue.size(),
                writeQueue.size());
        checkState(readQueue.isEmpty());
        checkState(writeQueue.size() == queueSize);
        for (ByteBuffer byteBuffer : writeQueue) {
            checkState(byteBuffer.position() == 0);
            checkState(byteBuffer.remaining() == bufferCapacity);
        }
    }

    public void stopBufferData() throws IOException {
        try {
            // add Empty buffer as finish flag.
            boolean isEmpty = false;
            if (currentWriteBuffer != null) {
                ((Buffer) currentWriteBuffer).flip();
                // check if the current write buffer is empty.
                isEmpty = currentWriteBuffer.limit() == 0;
                readQueue.put(currentWriteBuffer);
                currentWriteBuffer = null;
            }
            if (!isEmpty) {
                ByteBuffer byteBuffer = null;
                while (byteBuffer == null) {
                    checkErrorMessageByStreamLoad();
                    byteBuffer = writeQueue.poll(100, TimeUnit.MILLISECONDS);
                }
                ((Buffer) byteBuffer).flip();
                checkState(byteBuffer.limit() == 0);
                readQueue.put(byteBuffer);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public void write(byte[] buf) throws InterruptedException {
        int wPos = 0;
        do {
            while (currentWriteBuffer == null) {
                checkErrorMessageByStreamLoad();
                currentWriteBuffer = writeQueue.poll(100, TimeUnit.MILLISECONDS);
            }
            int available = currentWriteBuffer.remaining();
            int nWrite = Math.min(available, buf.length - wPos);
            currentWriteBuffer.put(buf, wPos, nWrite);
            wPos += nWrite;
            if (currentWriteBuffer.remaining() == 0) {
                ((Buffer) currentWriteBuffer).flip();
                readQueue.put(currentWriteBuffer);
                currentWriteBuffer = null;
            }
        } while (wPos != buf.length);
    }

    public int read(byte[] buf) throws InterruptedException {
        while (currentReadBuffer == null) {
            checkErrorMessageByStreamLoad();
            currentReadBuffer = readQueue.poll(100, TimeUnit.MILLISECONDS);
        }
        // add empty buffer as end flag
        if (currentReadBuffer.limit() == 0) {
            recycleBuffer(currentReadBuffer);
            currentReadBuffer = null;
            checkState(readQueue.isEmpty());
            return -1;
        }
        int available = currentReadBuffer.remaining();
        int nRead = Math.min(available, buf.length);
        currentReadBuffer.get(buf, 0, nRead);
        if (currentReadBuffer.remaining() == 0) {
            recycleBuffer(currentReadBuffer);
            currentReadBuffer = null;
        }
        return nRead;
    }

    private void checkErrorMessageByStreamLoad() {
        if (errorMessageByStreamLoad != null) {
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.STREAM_LOAD_FAILED, errorMessageByStreamLoad);
        }
    }

    private void recycleBuffer(ByteBuffer buffer) throws InterruptedException {
        ((Buffer) buffer).clear();
        while (!writeQueue.offer(buffer, 100, TimeUnit.MILLISECONDS)) {
            checkErrorMessageByStreamLoad();
        }
    }
}
