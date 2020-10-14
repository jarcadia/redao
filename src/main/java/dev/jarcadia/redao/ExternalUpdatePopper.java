package dev.jarcadia.redao;

import io.lettuce.core.RedisCommandInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class is responsible for popping update requests from the queue synchronously executing them
 */
class ExternalUpdatePopper implements Runnable, Closeable {

    private final Logger logger = LoggerFactory.getLogger(ExternalUpdatePopper.class);

    private final RedaoCommando rcommando;
    private final ExternalUpdatePopperRepository popperRepository;
    private final Procrastinator procrastinator;
    private final Thread thread;
    private final CountDownLatch drained;

    public ExternalUpdatePopper(RedaoCommando rcommando, ExternalUpdatePopperRepository popperRepository,
            Procrastinator procrastinator) {
        this.rcommando = rcommando;
        this.popperRepository = popperRepository;
        this.procrastinator = procrastinator;
        this.drained = new CountDownLatch(1);
        this.thread = new Thread(this, "update-popper");
        this.thread.setDaemon(false);
    }

    public void start() {
        this.thread.start();
    }

    @Override
    public void run() {
        logger.info("Starting blocking redao update popper");


        while (!Thread.interrupted()) {
            try {
                // Pop update
                String update = popperRepository.popUpdate();
                if (update == null) {
                    // Blocking timeout, no task popped
                    continue;
                } else {
                    try {
                        // Parse update and apply with rcommando
                        logger.trace("Popped update {} {}", update);

                    } catch (Throwable t) {

                    }
                }
            } catch(RedisCommandInterruptedException ex) {
                // This exception is thrown when this.thread is interrupted while performing blocking pop
                break;
            } catch (Throwable t) {
                logger.warn("Unexpected exception while popping task queue. Retrying in 1 second", t);
                try {
                    procrastinator.sleepFor(1000);
                }
                catch (InterruptedException e) {
                    break;
                }
            }
        }
        // Loop has exited which means all popped updates have completed
        logger.debug("Safely exited update popper");
        popperRepository.close();
        drained.countDown();
    }

    @Override
    public void close() {
        this.thread.interrupt();
    }

    protected CountDownLatch getDrainedLatch() {
        return drained;
    }

    protected void awaitDrainComplete() {
        try {
            this.drained.await();
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for updates to complete");
        }
    }

    protected void awaitDrainComplete(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            boolean drained = this.drained.await(timeout, unit);
            if (!drained) {
                throw new TimeoutException("Timeout while waiting for updates to complete");
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for updates to complete");
        }
    }
}
