package com.microsoft.azure.storage.table;

import com.microsoft.azure.storage.*;

public class RetryPolicyForServerSideThrottling extends RetryPolicy implements RetryPolicyFactory{

    public RetryPolicyForServerSideThrottling(){
        this(60*1000, RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT);
    }

    public RetryPolicyForServerSideThrottling(final int deltaBackoff, final int maxAttempts){
        super(deltaBackoff, maxAttempts);
    }
    @Override
    public RetryInfo evaluate(RetryContext retryContext, OperationContext operationContext) {
        boolean secondaryNotFound = this.evaluateLastAttemptAndSecondaryNotFound(retryContext);
        if (retryContext.getCurrentRetryCount() < this.maximumAttempts) {

            // If this method is called after a successful response, it means
            // we failed during the response body download. So, we should not
            // check for success codes here.
            int statusCode = retryContext.getLastRequestResult().getStatusCode();
            if (statusCode != 429 ) {
                return null;
            }

            final long retryInterval = Math.max(
                    Math.min(this.deltaBackoffIntervalInMs, RetryPolicy.DEFAULT_MAX_BACKOFF),
                    RetryPolicy.DEFAULT_MIN_BACKOFF);

            return this.evaluateRetryInfo(retryContext, secondaryNotFound, retryInterval);
        }

        return null;
    }

    @Override
    public RetryPolicy createInstance(OperationContext opContext) {
        return new RetryPolicyForServerSideThrottling(this.deltaBackoffIntervalInMs, this.maximumAttempts);
    }
}
