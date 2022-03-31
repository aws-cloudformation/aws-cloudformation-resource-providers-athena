package software.amazon.athena.workgroup;

import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.EngineVersion;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.ResultConfigurationUpdates;
import software.amazon.awssdk.services.athena.model.WorkGroupConfigurationUpdates;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static software.amazon.athena.workgroup.ResourceModel.TYPE_NAME;

final class HandlerUtils {

  public final static String DEFAULT_STATE = "ENABLED";
  public final static String DEFAULT_DESCRIPTION = "";

  private HandlerUtils() {
    // Intentionally left blank
  }

  /**
   * arn:${Partition}:athena:${Region}:${Account}:workgroup/${WorkGroupName}
   * See https://docs.aws.amazon.com/athena/latest/ug/workgroups-access.html
   * https://docs.aws.amazon.com/IAM/latest/UserGuide/list_amazonathena.html#amazonathena-resources-for-iam-policies
   */
  private static final String WORKGROUP_ARN_FORMAT = "arn:%s:athena:%s:%s:workgroup/%s";

  protected static final String WORKGROUP_NOT_FOUND = "not found";
  protected static final String WORKGROUP_ALREADY_CREATED = "already created";

  static String getWorkGroupArn(ResourceHandlerRequest<ResourceModel> request, String workGroupName) {
    return String.format(WORKGROUP_ARN_FORMAT,
        request.getAwsPartition(),
        request.getRegion(),
        request.getAwsAccountId(),
        workGroupName);
  }

  static RuntimeException translateAthenaException(AthenaException e, String resourceIdentifier) {
    if (e instanceof InvalidRequestException) {
      if (e.getMessage() != null && e.getMessage().contains(WORKGROUP_NOT_FOUND)) {
        return new CfnNotFoundException(TYPE_NAME, resourceIdentifier, e);
      } else if (e.getMessage() != null && e.getMessage().contains(WORKGROUP_ALREADY_CREATED)) {
        return new CfnAlreadyExistsException(TYPE_NAME, resourceIdentifier, e);
      } else {
        return new CfnInvalidRequestException(e.getMessage(), e);
      }
    } else if (e instanceof InternalServerException) {
      return new CfnGeneralServiceException(e.getMessage(), e);
    }

    return e;
  }

  static WorkGroupConfigurationUpdates getDefaultWorkGroupConfiguration() {
    return WorkGroupConfigurationUpdates.builder()
        .enforceWorkGroupConfiguration(true)
        .engineVersion(EngineVersion.builder().selectedEngineVersion("AUTO").build())
        .publishCloudWatchMetricsEnabled(true)
        .requesterPaysEnabled(false)
        .resultConfigurationUpdates(getDefaultResultConfiguration())
        .removeBytesScannedCutoffPerQuery(true)
        .build();
  }

  private static ResultConfigurationUpdates getDefaultResultConfiguration() {
    return ResultConfigurationUpdates.builder()
        .removeOutputLocation(true)
        .removeEncryptionConfiguration(true)
        .removeExpectedBucketOwner(true)
        .removeAclConfiguration(true)
        .build();
  }
}
