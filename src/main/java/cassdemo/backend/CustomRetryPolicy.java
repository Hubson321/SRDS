package cassdemo.backend;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class CustomRetryPolicy implements RetryPolicy {

    private static final int MAX_RETRIES = 3; // Maximum number of retry attempts


    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel consistencyLevel, int requiredResponses, int receivedResponses, boolean dataRetrieved, int retryCount) {
        // Retry on another host (tryNextHost) for read timeouts
        if (retryCount < MAX_RETRIES) {
            return RetryDecision.tryNextHost(consistencyLevel);
        } else {
            return RetryDecision.rethrow();
        }
    }

    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel consistency, int requiredReplica, int aliveReplica, int retryCount) {
        // Retry with consistency level ONE when there are not enough replicas for quorum
        if (consistency == ConsistencyLevel.QUORUM && retryCount < MAX_RETRIES) {
            return RetryDecision.retry(ConsistencyLevel.ONE);
        } else {
            return RetryDecision.rethrow();
        }
    }

    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel consistency, DriverException e, int retryCount) {
        // Retry on another host for request errors
        if (retryCount < MAX_RETRIES) {
            return RetryDecision.tryNextHost(consistency);
        } else {
            return RetryDecision.rethrow();
        }
    }

    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks,
            int receivedAcks, int nbRetry) {
        if (nbRetry < MAX_RETRIES) {
            return RetryDecision.tryNextHost(cl);
        } else {
            return RetryDecision.rethrow();
        }
    }

    @Override
    public void init(Cluster cluster) {
    }

















    @Override
    public void close() {
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }
}
