package software.amazon.athena.capacityreservation;

import com.amazonaws.arn.Arn;
import software.amazon.awssdk.services.athena.model.CancelCapacityReservationRequest;
import software.amazon.awssdk.services.athena.model.CreateCapacityReservationRequest;
import software.amazon.awssdk.services.athena.model.DeleteCapacityReservationRequest;
import software.amazon.awssdk.services.athena.model.GetCapacityAssignmentConfigurationRequest;
import software.amazon.awssdk.services.athena.model.GetCapacityReservationRequest;
import software.amazon.awssdk.services.athena.model.ListCapacityReservationsRequest;
import software.amazon.awssdk.services.athena.model.ListCapacityReservationsResponse;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.athena.model.PutCapacityAssignmentConfigurationRequest;
import software.amazon.awssdk.services.athena.model.TagResourceRequest;
import software.amazon.awssdk.services.athena.model.UntagResourceRequest;
import software.amazon.awssdk.services.athena.model.UpdateCapacityReservationRequest;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is a centralized placeholder for
 *  - api request construction
 *  - object translation to/from aws sdk
 *  - resource model construction for read/list handlers
 */

public class Translator {


  /**
   * Gets the name of the capacity reservation from the resource ARN
   * @param ARN - Capacity reservation ARN
   * @return Capacity reservation name
   */
  static String translateArnToCapacityReservationName(String ARN) {
    Arn arn = Arn.fromString(ARN);
    return arn.getResource().getResource();
  }

  /**
   * Builds the capacity reservation resource ARN for the given partition, region, account, and reservtion name
   * @param partition - partition of the capacity reservation
   * @param region - region of the capacity reservation
   * @param accountId - account id associated with capacity reservation
   * @param capacityReservationName - name of capacity reservation
   * @return
   */
  static String translateToCapacityReservationArn(String partition,
                                                  String region,
                                                  String accountId,
                                                  String capacityReservationName) {
    Arn arn = Arn.builder()
            .withPartition(partition)
            .withService("athena")
            .withRegion(region)
            .withAccountId(accountId)
            .withResource("capacity-reservation/" + capacityReservationName)
            .build();
    return arn.toString();
  }

  /**
   * Request to put a capacity assignment configuration
   * @param model resource model
   * @return PutCapacityAssignmentConfigurationRequest the service request to put a capacity assignment configuration
   */
  static PutCapacityAssignmentConfigurationRequest translateToPutCapacityAssignmentConfigRequest(
          final ResourceModel model) {
    return PutCapacityAssignmentConfigurationRequest.builder()
            .capacityReservationName(model.getName())
            .capacityAssignments(model.getCapacityAssignmentConfiguration()
                    .getCapacityAssignments()
                    .stream()
                    .map(capacityAssignment -> translateToCapacityAssignment(capacityAssignment))
                    .collect(Collectors.toList()))
            .build();
  }

  /**
   * Translates the CapacityAssignmentConfiguration SDK model object to a CapacityAssignmentConfiguration CFN resource model object
   * @param capacityAssignmentConfiguration - SDK model of CapacityAssignmentConfiguration
   * @return equivalent CapacityAssignmentConfiguration CFN resource model
   */
  static CapacityAssignmentConfiguration translateToCapacityAssignmentConfiguration(
          software.amazon.awssdk.services.athena.model.CapacityAssignmentConfiguration capacityAssignmentConfiguration) {
    return software.amazon.athena.capacityreservation.CapacityAssignmentConfiguration.builder()
            .capacityAssignments(capacityAssignmentConfiguration.capacityAssignments()
                    .stream()
                    .map(capacityAssignment -> translateToCapacityAssignment(capacityAssignment))
                    .collect(Collectors.toList()))
            .build();

  }

  /**
   * Translates the CapacityAssignment CFN resource model object to a CapacityAssignment SDK model object
   * @param capacityAssignment - CFN resource model of CapacityAssignment
   * @return equivalent CapacityAssignment SDK model
   */
  static software.amazon.awssdk.services.athena.model.CapacityAssignment translateToCapacityAssignment(
          CapacityAssignment capacityAssignment) {
    return software.amazon.awssdk.services.athena.model.CapacityAssignment.builder()
            .workGroupNames(capacityAssignment.getWorkgroupNames())
            .build();
  }

  /**
   * Translates the CapacityAssignment SDK model object to a CapacityAssignment CFN resource model object
   * @param capacityAssignment - SDK model of CapacityAssignment
   * @return equivalent CapacityAssignment CFN resource model
   */
  static CapacityAssignment  translateToCapacityAssignment(
          software.amazon.awssdk.services.athena.model.CapacityAssignment capacityAssignment) {
    return CapacityAssignment.builder()
            .workgroupNames(capacityAssignment.workGroupNames())
            .build();
  }

  /**
   * Request to create a capacity reservation
   * @param model resource model
   * @return CreateCapacityReservationRequest the service request to create a capacity reservation
   */
  static CreateCapacityReservationRequest translateToCreateCapacityReservationRequest(final ResourceModel model,
                                                                                      Set<Tag> tags) {
    return CreateCapacityReservationRequest.builder()
            .name(model.getName())
            .targetDpus(model.getTargetDpus().intValue())
            .tags(tags.isEmpty() ? null : tags.stream()
                    .map(tag -> translateToTag(tag))
                    .collect(Collectors.toList()))
            .build();
  }

  /**
   * Translates the Tag CFN resource model object to a Tag SDK model object
   * @param tag - CFN resource model of Tag
   * @return equivalent Tag SDK model
   */
  static software.amazon.awssdk.services.athena.model.Tag translateToTag(Tag tag) {
    return software.amazon.awssdk.services.athena.model.Tag.builder()
            .key(tag.getKey())
            .value(tag.getValue())
            .build();
  }

  /**
   * Translates the Tag SDK model object to a Tag CFN resource model object
   * @param tag - SDK model of Tag
   * @return equivalent Tag CFN resource model
   */
  static Tag translateToTag(software.amazon.awssdk.services.athena.model.Tag tag) {
    return Tag.builder()
            .key(tag.key())
            .value(tag.value())
            .build();
  }

  /**
   * Request to read a capacity reservation
   * @param model resource model
   * @return GetCapacityReservationRequest the service request to describe a capacity reservation
   */
  static GetCapacityReservationRequest translateToGetCapacityReservationRequest(final ResourceModel model) {
    return GetCapacityReservationRequest.builder()
            .name(model.getName())
            .build();
  }

  /**
   * Request to read a capacity assignment configuration
   * @param model resource model
   * @return GetCapacityAssignmentConfigurationRequest the service request to get a capacity assignment configuration
   */
  static GetCapacityAssignmentConfigurationRequest translateToGetCapacityAssignmentConfigRequest(final ResourceModel model) {
    return GetCapacityAssignmentConfigurationRequest.builder()
            .capacityReservationName(model.getName())
            .build();
  }

  /**
   * Request to list tags for a given resource
   * @param model resource model
   * @return ListTagsForResourceRequest the service request to list tags for the resource
   */
  static ListTagsForResourceRequest translateToListTagsForResourceRequest(final ResourceModel model) {
    return ListTagsForResourceRequest.builder()
            .resourceARN(model.getArn())
            .maxResults(100)
            .build();
  }

  /**
   * Request to list tags for a given resource with a specified next token
   * @param model resource model
   * @param nextToken next token for pagination
   * @return ListTagsForResourceRequest the service request to list tags for the resource
   */
  static ListTagsForResourceRequest translateToListTagsForResourceRequest(final ResourceModel model, String nextToken) {
    return ListTagsForResourceRequest.builder()
            .resourceARN(model.getArn())
            .maxResults(100)
            .nextToken(nextToken)
            .build();
  }

  /**
   * Request to cancel a capacity reservation
   * @param model resource model
   * @return CancelCapacityReservationRequest the request to cancel a capacity reservation
   */
  static CancelCapacityReservationRequest translateToCancelCapacityReservationRequest(final ResourceModel model) {
    return CancelCapacityReservationRequest.builder()
            .name(model.getName())
            .build();
  }

  /**
   * Request to delete a capacity reservation
   * @param model resource model
   * @return DeleteCapacityReservationRequest the request to delete a capacity reservation
   */
  static DeleteCapacityReservationRequest translateToDeleteCapacityReservationRequest(final ResourceModel model) {
    return DeleteCapacityReservationRequest.builder()
            .name(model.getName())
            .build();
  }

  /**
   * Request to update capacity reservation with new target DPUs
   * @param model resource model
   * @return UpdateCapacityReservationRequest the service request to update a capacity reservation
   */
  static UpdateCapacityReservationRequest translateToUpdateCapacityReservationRequest(final ResourceModel model) {
    return UpdateCapacityReservationRequest.builder()
            .name(model.getName())
            .targetDpus(model.getTargetDpus().intValue())
            .build();
  }

  /**
   * Request to list resources
   * @param nextToken token passed to the aws service list resources request
   * @return awsRequest the aws service request to list resources within aws account
   */
  static ListCapacityReservationsRequest translateToListRequest(final String nextToken) {
    return ListCapacityReservationsRequest.builder()
            .nextToken(nextToken)
            .build();
  }

  /**
   * Translates resource objects from sdk into a resource model (primary identifier only)
   * @param listResponse the aws service describe resource response
   * @return list of resource models
   */
  static List<ResourceModel> translateFromListRequest(
          final ListCapacityReservationsResponse listResponse,
          ResourceHandlerRequest<ResourceModel> request) {
    return streamOfOrEmpty(listResponse.capacityReservations())
        .map(reservation -> ResourceModel.builder()
                .arn(Translator.translateToCapacityReservationArn(request.getAwsPartition(),
                        request.getRegion(), request.getAwsAccountId(), reservation.name()))
            .build())
        .collect(Collectors.toList());
  }

  private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
    return Optional.ofNullable(collection)
        .map(Collection::stream)
        .orElseGet(Stream::empty);
  }

  /**
   * Request to add tags to a resource
   * @param model resource model
   * @return awsRequest the aws service request to create a resource
   */
  static TagResourceRequest tagResourceRequest(final ResourceModel model, final Map<String, String> addedTags) {
    Objects.requireNonNull(model.getArn());
    return TagResourceRequest.builder()
            .resourceARN(model.getArn())
            .tags(addedTags.keySet().stream()
                    .map(tagKey -> software.amazon.awssdk.services.athena.model.Tag.builder()
                            .key(tagKey)
                            .value(addedTags.get(tagKey))
                            .build())
                    .collect(Collectors.toList()))
            .build();
  }

  /**
   * Request to add tags to a resource
   * @param model resource model
   * @return awsRequest the aws service request to create a resource
   */
  static UntagResourceRequest untagResourceRequest(final ResourceModel model, final Set<String> removedTags) {
    Objects.requireNonNull(model.getArn());
    return UntagResourceRequest.builder()
            .resourceARN(model.getArn())
            .tagKeys(removedTags)
            .build();
  }
}
