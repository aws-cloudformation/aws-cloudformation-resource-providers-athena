package software.amazon.athena.datacatalog;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.DataCatalog;
import software.amazon.awssdk.services.athena.model.GetDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.GetDataCatalogResponse;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.ResourceNotFoundException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

class HandlerUtils {
  /**
   * DataCatalog ARN is of the following format:
   * arn:${Partition}:athena:${Region}:${Account}:datacatalog/${DataCatalogName}
   */
  private static final String DATACATALOG_ARN_FORMAT = "arn:%s:athena:%s:%s:datacatalog/%s";

  public static final String RESOURCE_TYPE = "AWS::Athena::DataCatalog";

  static String getDatacatalogArn(ResourceHandlerRequest<ResourceModel> request, String catalogName) {
    return String.format(DATACATALOG_ARN_FORMAT,
        request.getAwsPartition(),
        request.getRegion(),
        request.getAwsAccountId(),
        catalogName);
  }

  static RuntimeException handleExceptions(AthenaException e, String identifier) {
    if (e instanceof InvalidRequestException && e.getMessage()!= null &&
        e.getMessage().contains("has already been created")) {
      return new CfnAlreadyExistsException(e);
    }  else if ((e instanceof InvalidRequestException && e.getMessage()!= null &&
        e.getMessage().contains("was not found"))
        || e instanceof ResourceNotFoundException) {
      return new CfnNotFoundException(RESOURCE_TYPE, identifier, e);
    } else {
      return e;
    }
  }

  static DataCatalog getDataCatalog(ProxyClient<AthenaClient> client, ResourceModel model) {
    GetDataCatalogRequest getDataCatalogRequest = Translator.getDataCatalogRequest(model);
    GetDataCatalogResponse getDataCatalogResponse = client.injectCredentialsAndInvokeV2(
            getDataCatalogRequest, client.client()::getDataCatalog);
    return getDataCatalogResponse.dataCatalog();
  }
}
