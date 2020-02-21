package software.amazon.athena.workgroup;

import software.amazon.awssdk.services.athena.model.ResultConfigurationUpdates;
import software.amazon.awssdk.services.athena.model.WorkGroupConfiguration;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.EncryptionConfiguration;
import software.amazon.awssdk.services.athena.model.WorkGroupConfigurationUpdates;

import java.util.ArrayList;
import java.util.List;

class Translator {
  List<software.amazon.awssdk.services.athena.model.Tag> createSdkTagsFromCfnTags(List<software.amazon.athena.workgroup.Tag> cfnTags) {
    List<software.amazon.awssdk.services.athena.model.Tag> sdkTags = new ArrayList<>();
    cfnTags.forEach(q -> sdkTags.add(
      software.amazon.awssdk.services.athena.model.Tag.builder()
        .key(q.getKey())
        .value(q.getValue())
        .build()));
    return sdkTags;
  }

  WorkGroupConfiguration createSdkWorkgroupConfigurationFromCfnConfiguration(software.amazon.athena.workgroup.WorkGroupConfiguration cfnConfiguration) {
    return WorkGroupConfiguration.builder()
      .bytesScannedCutoffPerQuery(cfnConfiguration.getBytesScannedCutoffPerQuery() != null ? cfnConfiguration.getBytesScannedCutoffPerQuery().longValue() : null)
      .enforceWorkGroupConfiguration(cfnConfiguration.getEnforceWorkGroupConfiguration())
      .publishCloudWatchMetricsEnabled(cfnConfiguration.getPublishCloudWatchMetricsEnabled())
      .requesterPaysEnabled(cfnConfiguration.getRequesterPaysEnabled())
      .resultConfiguration(cfnConfiguration.getResultConfiguration() != null ? createSdkResultConfigurationFromCfnConfiguration(cfnConfiguration.getResultConfiguration()) : null)
      .build();
  }

  private ResultConfiguration createSdkResultConfigurationFromCfnConfiguration(software.amazon.athena.workgroup.ResultConfiguration resultConfiguration) {
    return ResultConfiguration.builder()
      .encryptionConfiguration(resultConfiguration.getEncryptionConfiguration() != null ? createSdkEncryptionConfigurationFromCfnConfiguration(resultConfiguration.getEncryptionConfiguration()) : null)
      .outputLocation(resultConfiguration.getOutputLocation())
      .build();
  }

  private EncryptionConfiguration createSdkEncryptionConfigurationFromCfnConfiguration(software.amazon.athena.workgroup.EncryptionConfiguration encryptionConfiguration) {
    return EncryptionConfiguration.builder()
      .encryptionOption(encryptionConfiguration.getEncryptionOption())
      .kmsKey(encryptionConfiguration.getKmsKey())
      .build();
  }

  WorkGroupConfigurationUpdates createSdkConfigurationUpdatesFromCfnConfigurationUpdates(software.amazon.athena.workgroup.WorkGroupConfigurationUpdates configuration) {
    return WorkGroupConfigurationUpdates.builder()
      .bytesScannedCutoffPerQuery(configuration.getBytesScannedCutoffPerQuery() != null ? configuration.getBytesScannedCutoffPerQuery().longValue() : null)
      .enforceWorkGroupConfiguration(configuration.getEnforceWorkGroupConfiguration())
      .publishCloudWatchMetricsEnabled(configuration.getPublishCloudWatchMetricsEnabled())
      .requesterPaysEnabled(configuration.getRequesterPaysEnabled())
      .removeBytesScannedCutoffPerQuery(configuration.getRemoveBytesScannedCutoffPerQuery())
      .resultConfigurationUpdates(configuration.getResultConfigurationUpdates() != null ?
        createSdkResultConfigurationUpdatesFromCfnConfigurationUpdate(configuration.getResultConfigurationUpdates()) : null)
      .build();
  }

  private ResultConfigurationUpdates createSdkResultConfigurationUpdatesFromCfnConfigurationUpdate(software.amazon.athena.workgroup.ResultConfigurationUpdates resultConfigurationUpdate) {
    return ResultConfigurationUpdates.builder()
      .encryptionConfiguration(resultConfigurationUpdate.getEncryptionConfiguration() != null ?
        createSdkEncryptionConfigurationFromCfnConfiguration(resultConfigurationUpdate.getEncryptionConfiguration()) : null)
      .outputLocation(resultConfigurationUpdate.getOutputLocation())
      .removeEncryptionConfiguration(resultConfigurationUpdate.getRemoveEncryptionConfiguration())
      .removeOutputLocation(resultConfigurationUpdate.getRemoveOutputLocation())
      .build();
  }

  software.amazon.athena.workgroup.WorkGroupConfiguration createCfnWorkgroupConfigurationFromSdkConfiguration(WorkGroupConfiguration sdkConfiguration) {
    return software.amazon.athena.workgroup.WorkGroupConfiguration.builder()
      .bytesScannedCutoffPerQuery(sdkConfiguration.bytesScannedCutoffPerQuery() != null ? sdkConfiguration.bytesScannedCutoffPerQuery().intValue() : null)
      .enforceWorkGroupConfiguration(sdkConfiguration.enforceWorkGroupConfiguration())
      .publishCloudWatchMetricsEnabled(sdkConfiguration.publishCloudWatchMetricsEnabled())
      .requesterPaysEnabled(sdkConfiguration.requesterPaysEnabled())
      .resultConfiguration(sdkConfiguration.resultConfiguration() != null ? createCfnResultConfigurationFromSdkConfiguration(sdkConfiguration.resultConfiguration())
        : null)
      .build();
  }

  private software.amazon.athena.workgroup.ResultConfiguration createCfnResultConfigurationFromSdkConfiguration(ResultConfiguration resultConfiguration) {
    return software.amazon.athena.workgroup.ResultConfiguration.builder()
      .encryptionConfiguration(resultConfiguration.encryptionConfiguration() != null ? createCfnEncryptionConfigurationFromSdkConfiguration(resultConfiguration.encryptionConfiguration()) : null)
      .outputLocation(resultConfiguration.outputLocation())
      .build();
  }

  private software.amazon.athena.workgroup.EncryptionConfiguration createCfnEncryptionConfigurationFromSdkConfiguration(EncryptionConfiguration encryptionConfiguration) {
    return software.amazon.athena.workgroup.EncryptionConfiguration.builder()
      .encryptionOption(encryptionConfiguration.encryptionOption().toString())
      .kmsKey(encryptionConfiguration.kmsKey())
      .build();
  }
}
