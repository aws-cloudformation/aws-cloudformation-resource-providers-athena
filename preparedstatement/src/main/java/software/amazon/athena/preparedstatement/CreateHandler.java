package software.amazon.athena.preparedstatement;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.CreatePreparedStatementResponse;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.ResourceNotFoundException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class CreateHandler extends BaseHandlerStd {
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
                        client.injectCredentialsAndInvokeV2(
                            awsRequest, client.client()::getPreparedStatement);
                        logger.log(String.format("%s has been read successfully.", ResourceModel.TYPE_NAME));
                        throw new CfnAlreadyExistsException(ResourceModel.TYPE_NAME, awsRequest.statementName());
                    })

                    .handleError((awsRequest, exception, client, model, context) -> {
                        if (exception instanceof ResourceNotFoundException) {
                            return ProgressEvent.progress(model, context);
                        } else if (exception instanceof InvalidRequestException &&
                            exception.getMessage().contains("is not found")) {
                            return ProgressEvent.progress(model, context);
                        }
                        throw exception;
                    })
                    .progress()
            )

            .then(progress ->
                proxy.initiate("AWS-Athena-PreparedStatement::Create",
                    proxyClient,progress.getResourceModel(), progress.getCallbackContext())
                    .translateToServiceRequest(Translator::translateToCreateRequest)
                    .makeServiceCall((awsRequest, client) -> {
                        CreatePreparedStatementResponse awsResponse;
                        try {
                          awsResponse = client.injectCredentialsAndInvokeV2(
                              awsRequest, client.client()::createPreparedStatement);
                        } catch (final AwsServiceException e) {
                            throw new CfnGeneralServiceException(ResourceModel.TYPE_NAME, e);
                        }

                        logger.log(String.format("%s successfully created.", ResourceModel.TYPE_NAME));
                        return awsResponse;
                    })
                    .progress()
                )

            .then(progress -> new ReadHandler()
                .handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
