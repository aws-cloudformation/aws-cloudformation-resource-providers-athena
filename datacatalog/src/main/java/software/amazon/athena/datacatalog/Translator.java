package software.amazon.athena.datacatalog;

import java.util.*;

import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import software.amazon.awssdk.services.athena.model.CreateDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.DataCatalogSummary;
import software.amazon.awssdk.services.athena.model.DeleteDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.GetDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.UpdateDataCatalogRequest;

import static java.util.stream.Collectors.toMap;


class Translator {

  static CreateDataCatalogRequest createDataCatalogRequest(ResourceModel resourceModel,
                                                           Map<String, String> stackTags) {

    return CreateDataCatalogRequest.builder()
        .name(resourceModel.getName())
        .type(resourceModel.getType())
        .description(resourceModel.getDescription())
        .parameters(resourceModel.getParameters())
        .tags(convertToAthenaSdkTags(consolidateTags(resourceModel.getTags(), stackTags)))
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
    return UpdateDataCatalogRequest.builder()
        .description(resourceModel.getDescription())
        .name(resourceModel.getName())
        .parameters(resourceModel.getParameters())
        .type(resourceModel.getType())
        .build();
  }

  static List<software.amazon.awssdk.services.athena.model.Tag> convertToAthenaSdkTags(
          final Map<String, String> cfnTags) {
    if (MapUtils.isEmpty(cfnTags)) return null;
    List<software.amazon.awssdk.services.athena.model.Tag> sdkTags = new ArrayList<>();
    cfnTags.forEach((key, value) -> sdkTags.add(
            software.amazon.awssdk.services.athena.model.Tag.builder()
                    .key(key)
                    .value(value)
                    .build()));
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
    return ResourceModel.builder()
        .name(summary.catalogName())
        .type(summary.typeAsString())
        .build();
  }

  /**
   * Use this method in the UPDATE workflow, since aws prefixed tags will never change
   * This will combine the resource level tags and stack level tags
   * @param resourceTags List of resource tags.
   * @param stackLevelTags stack level tags specified by the customer to be placed on each resource
   * @return a consolidated map including all tags
   */
  public static Map<String, String> consolidateTags(
          final Collection<Tag> resourceTags,
          final Map<String, String> stackLevelTags) {

    Map<String, String> resourceLevelTags = resourceTags == null ?
            Collections.emptyMap() : resourceTags.stream().collect(toMap(Tag::getKey, Tag::getValue));

    Map<String, String> consolidatedTags = Maps.newHashMap();
    if (MapUtils.isNotEmpty(stackLevelTags)) consolidatedTags.putAll(stackLevelTags);

    // Resource tags will override stack level tags with same keys.
    if (MapUtils.isNotEmpty(resourceLevelTags)) consolidatedTags.putAll(resourceLevelTags);

    return consolidatedTags;
  }
}
