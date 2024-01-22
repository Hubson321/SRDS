package cassdemo.backend;

import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class CustomRetryPolicy implements RetryPolicy {

    private static final int MAX_RETRIES = 3; // Maximum number of retry attempts
    private AtomicInteger readTimeoutMetric = new AtomicInteger(0);
    private AtomicInteger writeTimeoutMetric = new AtomicInteger(0);
    private AtomicInteger noQuorumMetric = new AtomicInteger(0);
    private AtomicInteger anyErrorsMetric = new AtomicInteger(0);

    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel consistencyLevel, int requiredResponses, int receivedResponses, boolean dataRetrieved, int retryCount) {
        // Retry on another host (tryNextHost) for read timeouts
        // System.out.println("[onReadTimeout] Inside function");

        if (retryCount < MAX_RETRIES) {
            readTimeoutMetric.incrementAndGet();
            return RetryDecision.tryNextHost(consistencyLevel);
        } else {
            return RetryDecision.rethrow();
        }
    }

    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel consistency, int requiredReplica, int aliveReplica, int retryCount) {
        // Retry with consistency level ONE when there are not enough replicas for quorum
        // System.out.println("[onUnavailable] Inside function");
        
        if (consistency == ConsistencyLevel.QUORUM && retryCount >= MAX_RETRIES) {
            noQuorumMetric.incrementAndGet();
            return RetryDecision.retry(ConsistencyLevel.ONE);
        } else {
            return RetryDecision.retry(ConsistencyLevel.QUORUM);
        }
    }

    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel consistency, DriverException e, int retryCount) {
        // Retry on another host for request errors
        // System.out.println("[onRequestError] Inside function");
        if (retryCount < MAX_RETRIES) {
            anyErrorsMetric.incrementAndGet();
            return RetryDecision.tryNextHost(consistency);
        } else {
            return RetryDecision.rethrow();
        }
    }

    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks,
            int receivedAcks, int nbRetry) {
        // System.out.println("[onWriteTimeout] Inside function");
        if (nbRetry < MAX_RETRIES) {
            writeTimeoutMetric.incrementAndGet();
            return RetryDecision.tryNextHost(cl);
        } else {
            return RetryDecision.rethrow();
        }
    }

    @Override
    public void init(Cluster cluster) {
    }


    public void printErrors() {
        System.out.println("----------------------------------------------------------");
        System.out.println("Read timeout metric: " + readTimeoutMetric);
        System.out.println("Write timeout metric: " + writeTimeoutMetric);
        System.out.println("No Quorum metric: " + noQuorumMetric);
        System.out.println("Any error metric: " + anyErrorsMetric);
        System.out.println("----------------------------------------------------------");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }
}
