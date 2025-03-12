package software.amazon.athena.datacatalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import software.amazon.awssdk.services.athena.model.CreateDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.DataCatalogType;
import software.amazon.awssdk.services.athena.model.DataCatalogSummary;
import software.amazon.awssdk.services.athena.model.DataCatalogType;
import software.amazon.awssdk.services.athena.model.DeleteDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.GetDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.TagResourceRequest;
import software.amazon.awssdk.services.athena.model.UntagResourceRequest;
import software.amazon.awssdk.services.athena.model.UpdateDataCatalogRequest;

class Translator {

  static CreateDataCatalogRequest createDataCatalogRequest(ResourceModel resourceModel, Map<String, String> stackTags) {
    return CreateDataCatalogRequest.builder()
        .name(resourceModel.getName())
        .type(resourceModel.getType())
        .description(resourceModel.getDescription())
        .parameters(resourceModel.getParameters())
        .tags(convertToAthenaSdkTags(resourceModel.getTags(), stackTags))
        .build();
  }

  static DeleteDataCatalogRequest deleteDataCatalogRequest(ResourceModel resourceModel) {
    return DeleteDataCatalogRequest.builder()
        .name(resourceModel.getName())
        .build();
  }

  static GetDataCatalogRequest getDataCatalogRequest(ResourceModel resourceModel) {
    return GetDataCatalogRequest.builder()
        .name(resourceModel.getName())
        .build();
  }

  static UpdateDataCatalogRequest updateDataCatalogRequest(ResourceModel resourceModel) {
    if (resourceModel.getType().equals(DataCatalogType.FEDERATED.name())) {
      return UpdateDataCatalogRequest.builder()
              .name(resourceModel.getName())
              .type(resourceModel.getType())
              .description(resourceModel.getDescription())
              .build();
    }
    return UpdateDataCatalogRequest.builder()
            .description(resourceModel.getDescription())
            .name(resourceModel.getName())
            .parameters(resourceModel.getParameters())
            .type(resourceModel.getType())
            .build();
  }

  static List<software.amazon.awssdk.services.athena.model.Tag> convertToAthenaSdkTags(
          final Collection<Tag> resourceTags, final Map<String, String> stackLevelTags) {

    if (CollectionUtils.isEmpty(resourceTags) && MapUtils.isEmpty(stackLevelTags)) {
      return null;
    }
    Map<String, String> consolidatedTags = Maps.newHashMap();
    if (MapUtils.isNotEmpty(stackLevelTags)) {
      consolidatedTags.putAll(stackLevelTags);
    }

    // Resource tags will override stack level tags with same keys.
    if (CollectionUtils.isNotEmpty(resourceTags)) {
      resourceTags.forEach(tag -> consolidatedTags.put(tag.getKey(), tag.getValue()));
    }

    List<software.amazon.awssdk.services.athena.model.Tag> sdkTags = new ArrayList<>();
    consolidatedTags.forEach((key, value) -> sdkTags.add(
            software.amazon.awssdk.services.athena.model.Tag.builder().key(key).value(value).build()));
    return sdkTags;
  }

  static List<software.amazon.athena.datacatalog.Tag> convertToResourceModelTags(
        List<software.amazon.awssdk.services.athena.model.Tag> athenaSdkTags) {
    if (athenaSdkTags == null) return null;
    List<software.amazon.athena.datacatalog.Tag> resourceModelTags = new ArrayList<>();
    athenaSdkTags.forEach(q -> resourceModelTags.add(
        software.amazon.athena.datacatalog.Tag.builder()
            .key(q.key())
            .value(q.value())
            .build()));

    return resourceModelTags;
  }

  static ResourceModel getModelFromDataCatalogSummary(DataCatalogSummary summary) {
    ResourceModel.ResourceModelBuilder resourceModelBuilder = ResourceModel.builder()
            .name(summary.catalogName())
            .type(summary.typeAsString())
            .status(summary.statusAsString());
    if (summary.type().equals(DataCatalogType.FEDERATED)) {
      resourceModelBuilder.connectionType(summary.connectionTypeAsString());
      resourceModelBuilder.error(summary.error() == null ? "" : summary.error());
    }

    return resourceModelBuilder.build();
  }

  /**
   * Request to add/update tags to/of a resource
   * @param arn the ARN of the resource
   * @return the AWS service request to tag a resource
   */
  static TagResourceRequest tagResourceRequest(final String arn, final Map<String, String> addedTags) {
    Objects.requireNonNull(arn);
    return TagResourceRequest.builder()
            .resourceARN(arn)
            .tags(addedTags.keySet().stream()
                    .map(tagKey -> software.amazon.awssdk.services.athena.model.Tag.builder()
                            .key(tagKey)
                            .value(addedTags.get(tagKey))
                            .build())
                    .collect(Collectors.toList()))
            .build();
  }

  /**
   * Request to remove tags from a resource
   * @param arn the ARN of the resource
   * @return the AWS service request to untag a resource
   */
  static UntagResourceRequest untagResourceRequest(final String arn, final Set<String> removedTags) {
    Objects.requireNonNull(arn);
    return UntagResourceRequest.builder()
            .resourceARN(arn)
            .tagKeys(removedTags)
            .build();
  }
}
