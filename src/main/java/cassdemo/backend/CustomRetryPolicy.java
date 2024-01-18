package cassdemo.backend;

import java.util.concurrent.locks.ReentrantLock;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class CustomRetryPolicy implements RetryPolicy {

    private static final int MAX_RETRIES = 3; // Maximum number of retry attempts
    private int readTimeoutMetric = 0;
    private int writeTimeoutMetric = 0;
    private int noQuorumMetric = 0;
    private int anyErrorsMetric = 0;

    private ReentrantLock metricLock = new ReentrantLock(true);

    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel consistencyLevel, int requiredResponses, int receivedResponses, boolean dataRetrieved, int retryCount) {
        System.out.println("[onReadTimeout] Inside function");
        // Retry on another host (tryNextHost) for read timeouts
        metricLock.lock();
        try {
             if (retryCount < MAX_RETRIES) {
                readTimeoutMetric++;
                return RetryDecision.tryNextHost(consistencyLevel);
            } else {
                return RetryDecision.rethrow();
            }
        } finally {
            metricLock.unlock();
        }
    }

    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel consistency, int requiredReplica, int aliveReplica, int retryCount) {
        System.out.println("[onUnavailable] Inside function");
        // Retry with consistency level ONE when there are not enough replicas for quorum
        metricLock.lock();
        try{
            if (consistency == ConsistencyLevel.QUORUM && retryCount >= MAX_RETRIES) {
                noQuorumMetric++;
                return RetryDecision.retry(ConsistencyLevel.ONE);
            } else {
                return RetryDecision.retry(ConsistencyLevel.QUORUM);
            }
        }finally{
            metricLock.unlock();
        }
    }

    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel consistency, DriverException e, int retryCount) {
        System.out.println("[onRequestError] Inside function");
        // Retry on another host for request errors
        metricLock.lock();
        try{
             if (retryCount < MAX_RETRIES) {
                anyErrorsMetric++;
               return RetryDecision.tryNextHost(consistency);
            } else {
                return RetryDecision.rethrow();
            }
        }finally{
            metricLock.unlock();
        }
    }

    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks,
            int receivedAcks, int nbRetry) {
        System.out.println("[onWriteTimeout] Inside function");
        metricLock.lock();
        try {
            if (nbRetry < MAX_RETRIES) {
                writeTimeoutMetric++;
                return RetryDecision.tryNextHost(cl);
            } else {
                return RetryDecision.rethrow();
            }
        }finally{
            metricLock.unlock();
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
