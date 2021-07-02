package software.amazon.athena.workgroup;

import software.amazon.awssdk.services.athena.model.EncryptionConfiguration;
import software.amazon.awssdk.services.athena.model.EngineVersion;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.ResultConfigurationUpdates;
import software.amazon.awssdk.services.athena.model.WorkGroupConfiguration;
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

  List<software.amazon.athena.workgroup.Tag> createCfnTagsFromSdkTags(List<software.amazon.awssdk.services.athena.model.Tag> sdkTags) {
    List<software.amazon.athena.workgroup.Tag> cfnTags = new ArrayList<>();
    sdkTags.forEach(tag -> cfnTags.add(
        software.amazon.athena.workgroup.Tag.builder()
          .key(tag.key())
          .value(tag.value())
          .build()));
    return cfnTags;
  }

  WorkGroupConfiguration createSdkWorkgroupConfigurationFromCfnConfiguration(software.amazon.athena.workgroup.WorkGroupConfiguration cfnConfiguration) {
    return WorkGroupConfiguration.builder()
      .bytesScannedCutoffPerQuery(cfnConfiguration.getBytesScannedCutoffPerQuery())
      .enforceWorkGroupConfiguration(cfnConfiguration.getEnforceWorkGroupConfiguration())
      .publishCloudWatchMetricsEnabled(cfnConfiguration.getPublishCloudWatchMetricsEnabled())
      .requesterPaysEnabled(cfnConfiguration.getRequesterPaysEnabled())
      .resultConfiguration(cfnConfiguration.getResultConfiguration() != null ? createSdkResultConfigurationFromCfnConfiguration(cfnConfiguration.getResultConfiguration()) : null)
      .engineVersion(cfnConfiguration.getEngineVersion() != null ? createSdkEngineVersionFromCfnConfiguration(cfnConfiguration.getEngineVersion()) : null)
      .build();
  }

  private EngineVersion createSdkEngineVersionFromCfnConfiguration(software.amazon.athena.workgroup.EngineVersion engineVersion) {
    return EngineVersion.builder()
            .selectedEngineVersion(engineVersion.getSelectedEngineVersion())
            .effectiveEngineVersion(engineVersion.getEffectiveEngineVersion())
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
      .bytesScannedCutoffPerQuery(configuration.getBytesScannedCutoffPerQuery())
      .enforceWorkGroupConfiguration(configuration.getEnforceWorkGroupConfiguration())
      .publishCloudWatchMetricsEnabled(configuration.getPublishCloudWatchMetricsEnabled())
      .requesterPaysEnabled(configuration.getRequesterPaysEnabled())
      .removeBytesScannedCutoffPerQuery(configuration.getRemoveBytesScannedCutoffPerQuery())
      .resultConfigurationUpdates(configuration.getResultConfigurationUpdates() != null ?
        createSdkResultConfigurationUpdatesFromCfnConfigurationUpdate(configuration.getResultConfigurationUpdates()) : null)
      .engineVersion(configuration.getEngineVersion() != null ? createSdkEngineVersionFromCfnConfiguration(configuration.getEngineVersion()) : null)
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
      .bytesScannedCutoffPerQuery(sdkConfiguration.bytesScannedCutoffPerQuery())
      .enforceWorkGroupConfiguration(sdkConfiguration.enforceWorkGroupConfiguration())
      .publishCloudWatchMetricsEnabled(sdkConfiguration.publishCloudWatchMetricsEnabled())
      .requesterPaysEnabled(sdkConfiguration.requesterPaysEnabled())
      .resultConfiguration(sdkConfiguration.resultConfiguration() != null ? createCfnResultConfigurationFromSdkConfiguration(sdkConfiguration.resultConfiguration())
        : null)
      .engineVersion(sdkConfiguration.engineVersion() != null ? createCfnEngineVersionFromSdkConfiguration(sdkConfiguration.engineVersion())
              : null)
      .build();
  }

  private software.amazon.athena.workgroup.EngineVersion createCfnEngineVersionFromSdkConfiguration(EngineVersion engineVersion) {
    return software.amazon.athena.workgroup.EngineVersion.builder()
            .selectedEngineVersion(engineVersion.selectedEngineVersion())
            .effectiveEngineVersion(engineVersion.effectiveEngineVersion())
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
