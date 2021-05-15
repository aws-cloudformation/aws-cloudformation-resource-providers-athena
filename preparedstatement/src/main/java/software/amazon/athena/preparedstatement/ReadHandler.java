package software.amazon.athena.preparedstatement;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.GetPreparedStatementResponse;
import software.amazon.awssdk.services.athena.model.ResourceNotFoundException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandlerStd {
    private Logger logger;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<AthenaClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        return proxy.initiate("AWS-Athena-PreparedStatement::Read",
            proxyClient, request.getDesiredResourceState(), callbackContext)
            .translateToServiceRequest(Translator::translateToReadRequest)
            .makeServiceCall((awsRequest, client) -> {
                GetPreparedStatementResponse awsResponse;
                try {
                  awsResponse = client.injectCredentialsAndInvokeV2(awsRequest, client.client()::getPreparedStatement);
                } catch (final AwsServiceException e) {
                    if (e instanceof ResourceNotFoundException) {
                      throw new CfnNotFoundException(e);
                    }
                    throw new CfnGeneralServiceException(ResourceModel.TYPE_NAME, e);
                } catch (Exception e) {
                  e.printStackTrace();
                  throw e;
                }

                logger.log(String.format("%s has successfully been read.", ResourceModel.TYPE_NAME));
                return awsResponse;
            })
            .done(awsResponse -> ProgressEvent.defaultSuccessHandler(Translator.translateFromReadResponse(awsResponse)));
    }
}
