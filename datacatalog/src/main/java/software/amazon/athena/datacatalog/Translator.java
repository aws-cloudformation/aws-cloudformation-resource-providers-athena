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
                                                           Map<String, String> resourceTags) {

    return CreateDataCatalogRequest.builder()
        .name(resourceModel.getName())
        .type(resourceModel.getType())
        .description(resourceModel.getDescription())
        .parameters(resourceModel.getParameters())
        .tags(convertToAthenaSdkTags(resourceTags))
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
}
