package software.amazon.athena.datacatalog;

import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.services.athena.model.CreateDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.DataCatalogSummary;
import software.amazon.awssdk.services.athena.model.DeleteDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.GetDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.UpdateDataCatalogRequest;

class Translator {

  static CreateDataCatalogRequest createDataCatalogRequest(ResourceModel resourceModel) {
    return CreateDataCatalogRequest.builder()
        .name(resourceModel.getName())
        .type(resourceModel.getType())
        .description(resourceModel.getDescription())
        .parameters(resourceModel.getParameters())
        .tags(convertToAthenaSdkTags(resourceModel.getTags()))
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
        List<software.amazon.athena.datacatalog.Tag> cfnResourceModelTags) {

    if (cfnResourceModelTags == null) return null;
    List<software.amazon.awssdk.services.athena.model.Tag> sdkTags = new ArrayList<>();
    cfnResourceModelTags.forEach(q -> sdkTags.add(
    software.amazon.awssdk.services.athena.model.Tag.builder()
        .key(q.getKey())
        .value(q.getValue())
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
