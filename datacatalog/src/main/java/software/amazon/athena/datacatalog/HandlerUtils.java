package software.amazon.athena.datacatalog;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.ResourceNotFoundException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

class HandlerUtils {
  /**
   * DataCatalog ARN is of the following format:
   * arn:${Partition}:athena:${Region}:${Account}:datacatalog/${DataCatalogName}
   */
  private static final String DATACATALOG_ARN_FORMAT = "arn:%s:athena:%s:%s:datacatalog/%s";

  public static final String RESOURCE_TYPE = "AWS::Athena::DataCatalog";

  private static final Map<String, String> NONDEFAULT_AWS_PARTITION = ImmutableMap.of(
      "cn-northwest-1", "aws-cn",
      "us-gov-east-1", "aws-us-gov",
      "us-iso-east-1", "aws-iso",
      "us-isob-east-1", "aws-iso-b"
  );

  static String getDatacatalogArn(ResourceHandlerRequest<ResourceModel> request, String catalogName) {
    return String.format(DATACATALOG_ARN_FORMAT,
        NONDEFAULT_AWS_PARTITION.getOrDefault(request.getRegion(), "aws"),
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
}
