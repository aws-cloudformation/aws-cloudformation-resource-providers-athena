package software.amazon.athena.capacityreservation;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.ListCapacityReservationsRequest;
import software.amazon.awssdk.services.athena.model.ListCapacityReservationsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;

public class ListHandler extends BaseHandlerStd {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<AthenaClient> proxyClient,
        final Logger logger) {

        final ListCapacityReservationsRequest listCapacityReservationsRequest =
                Translator.translateToListRequest(request.getNextToken());

        ListCapacityReservationsResponse listResponse =
                proxyClient.injectCredentialsAndInvokeV2(listCapacityReservationsRequest,
                        proxyClient.client()::listCapacityReservations);

        final List<ResourceModel> models = Translator.translateFromListRequest(listResponse, request);
        String nextToken = listResponse.nextToken();

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModels(models)
            .nextToken(nextToken)
            .status(OperationStatus.SUCCESS)
            .build();
    }
}
