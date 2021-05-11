package software.amazon.athena.preparedstatement;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.DeletePreparedStatementResponse;
import software.amazon.awssdk.services.athena.model.GetPreparedStatementResponse;
import software.amazon.awssdk.services.athena.model.ResourceNotFoundException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandlerStd {
    private Logger logger;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<AthenaClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
            .then(progress ->
                proxy.initiate("AWS-Athena-PreparedStatement::Create::PreExistanceCheck",
                    proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                    .translateToServiceRequest(Translator::translateToReadRequest)
                    .makeServiceCall((awsRequest, client) -> {
                        GetPreparedStatementResponse awsResponse = client.injectCredentialsAndInvokeV2(
                            awsRequest, client.client()::getPreparedStatement);
                        logger.log(String.format("%s has successfully been read.", ResourceModel.TYPE_NAME));
                        return awsResponse;
                    })
                    .handleError((awsRequest, exception, client, model, context) -> {
                        if (exception instanceof ResourceNotFoundException) {
                            return ProgressEvent.failed(model, context, HandlerErrorCode.NotFound,
                                exception.getMessage());
                        }
                        throw exception;
                    })
                    .progress()
            )
            .then(progress ->
                proxy.initiate("AWS-Athena-PreparedStatement::Delete", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                    .translateToServiceRequest(Translator::translateToDeleteRequest)
                    .makeServiceCall((awsRequest, client) -> {
                        DeletePreparedStatementResponse awsResponse;
                        try {
                            awsResponse = client.injectCredentialsAndInvokeV2(
                                awsRequest, client.client()::deletePreparedStatement);
                        } catch (final AwsServiceException e) {
                            throw new CfnGeneralServiceException(ResourceModel.TYPE_NAME, e);
                        }

                        logger.log(String.format("%s successfully deleted.", ResourceModel.TYPE_NAME));
                        return awsResponse;
                    })
                    .progress()
            )
            .then(progress -> ProgressEvent.defaultSuccessHandler(null));
    }
}
