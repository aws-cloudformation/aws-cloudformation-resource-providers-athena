package software.amazon.athena.workgroup;

import software.amazon.awssdk.services.athena.model.AclConfiguration;
import software.amazon.awssdk.services.athena.model.CustomerContentEncryptionConfiguration;
import software.amazon.awssdk.services.athena.model.ManagedQueryResultsConfiguration;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import software.amazon.awssdk.services.athena.model.EncryptionConfiguration;
import software.amazon.awssdk.services.athena.model.EngineVersion;
import software.amazon.awssdk.services.athena.model.ManagedQueryResultsConfigurationUpdates;
import software.amazon.awssdk.services.athena.model.ManagedQueryResultsEncryptionConfiguration;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.ResultConfigurationUpdates;
import software.amazon.awssdk.services.athena.model.WorkGroupConfiguration;
import software.amazon.awssdk.services.athena.model.WorkGroupConfigurationUpdates;
import software.amazon.awssdk.services.athena.model.MonitoringConfiguration;
import software.amazon.awssdk.services.athena.model.CloudWatchLoggingConfiguration;
import software.amazon.awssdk.services.athena.model.ManagedLoggingConfiguration;
import software.amazon.awssdk.services.athena.model.S3LoggingConfiguration;
import software.amazon.awssdk.services.athena.model.EngineConfiguration;
import software.amazon.awssdk.services.athena.model.Classification;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

class Translator {

  List<software.amazon.awssdk.services.athena.model.Tag> createConsolidatedSdkTagsFromCfnTags(
          final Collection<software.amazon.athena.workgroup.Tag> resourceTags, final Map<String, String> stackLevelTags) {
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
            .additionalConfiguration(cfnConfiguration.getAdditionalConfiguration())
            .executionRole(cfnConfiguration.getExecutionRole())
            .customerContentEncryptionConfiguration(cfnConfiguration.getCustomerContentEncryptionConfiguration() != null ?
                    createSdkCustomerContentEncryptionConfigurationFromCfnConfiguration(cfnConfiguration.getCustomerContentEncryptionConfiguration()) : null)
            .managedQueryResultsConfiguration(cfnConfiguration.getManagedQueryResultsConfiguration() != null ?
                    createSdkManagedQueryResultsConfigurationFromCfnConfiguration(cfnConfiguration.getManagedQueryResultsConfiguration()) : null)
            .engineConfiguration(cfnConfiguration.getEngineConfiguration() != null ?
                    createSdkEngineConfigurationFromCfnConfiguration(cfnConfiguration.getEngineConfiguration()) : null)
            .monitoringConfiguration(cfnConfiguration.getMonitoringConfiguration() != null ?
                    createSdkMonitoringConfigurationFromCfnConfiguration(cfnConfiguration.getMonitoringConfiguration()) : null)
            .build();
  }

  private EngineVersion createSdkEngineVersionFromCfnConfiguration(software.amazon.athena.workgroup.EngineVersion engineVersion) {
    return EngineVersion.builder()
            .selectedEngineVersion(engineVersion.getSelectedEngineVersion())
            .effectiveEngineVersion(engineVersion.getEffectiveEngineVersion())
            .build();
  }

  EngineConfiguration createSdkEngineConfigurationFromCfnConfiguration(software.amazon.athena.workgroup.EngineConfiguration engineConfiguration) {
    return EngineConfiguration.builder()
            .classifications(engineConfiguration.getClassifications() != null ?
                    createSdkClassificationsFromCfnClassifications(engineConfiguration.getClassifications()) : null)
            .build();
  }

  List<Classification> createSdkClassificationsFromCfnClassifications(List<software.amazon.athena.workgroup.Classification> cfnClassifications) {
    if (cfnClassifications == null) {
      return null;
    }
    List<Classification> sdkClassifications = new ArrayList<>();
    cfnClassifications.forEach(cfnClassification -> {
      sdkClassifications.add(createSdkClassificationFromCfnClassification(cfnClassification));
    });
    return sdkClassifications;
  }

  Classification createSdkClassificationFromCfnClassification(software.amazon.athena.workgroup.Classification cfnClassification) {
    return Classification.builder()
            .name(cfnClassification.getName())
            .properties(cfnClassification.getProperties())
            .build();
  }

  MonitoringConfiguration createSdkMonitoringConfigurationFromCfnConfiguration(software.amazon.athena.workgroup.MonitoringConfiguration cfnMonitoringConfiguration) {
    return MonitoringConfiguration.builder()
            .cloudWatchLoggingConfiguration(cfnMonitoringConfiguration.getCloudWatchLoggingConfiguration() != null ?
                    createSdkCloudWatchLoggingConfigurationFromCfnConfiguration(cfnMonitoringConfiguration.getCloudWatchLoggingConfiguration()) : null)
            .managedLoggingConfiguration(cfnMonitoringConfiguration.getManagedLoggingConfiguration() != null ?
                    createSdkManagedLoggingConfigurationFromCfnConfiguration(cfnMonitoringConfiguration.getManagedLoggingConfiguration()) : null)
            .s3LoggingConfiguration(cfnMonitoringConfiguration.getS3LoggingConfiguration() != null ?
                    createSdkS3LoggingConfigurationFromCfnConfiguration(cfnMonitoringConfiguration.getS3LoggingConfiguration()) : null)
            .build();
  }

  CloudWatchLoggingConfiguration createSdkCloudWatchLoggingConfigurationFromCfnConfiguration(software.amazon.athena.workgroup.CloudWatchLoggingConfiguration cfnCloudWatchLoggingConfiguration) {
    return CloudWatchLoggingConfiguration.builder()
            .enabled(cfnCloudWatchLoggingConfiguration.getEnabled())
            .logGroup(cfnCloudWatchLoggingConfiguration.getLogGroup())
            .logStreamNamePrefix(cfnCloudWatchLoggingConfiguration.getLogStreamNamePrefix())
            .logTypes(convertCfnLogTypesToSdkLogTypes(cfnCloudWatchLoggingConfiguration.getLogTypes()))
            .build();
  }

  ManagedLoggingConfiguration createSdkManagedLoggingConfigurationFromCfnConfiguration(software.amazon.athena.workgroup.ManagedLoggingConfiguration cfnManagedLoggingConfiguration) {
    return ManagedLoggingConfiguration.builder()
            .enabled(cfnManagedLoggingConfiguration.getEnabled())
            .kmsKey(cfnManagedLoggingConfiguration.getKmsKey())
            .build();
  }

  S3LoggingConfiguration createSdkS3LoggingConfigurationFromCfnConfiguration(software.amazon.athena.workgroup.S3LoggingConfiguration cfnS3LoggingConfiguration) {
    return S3LoggingConfiguration.builder()
            .enabled(cfnS3LoggingConfiguration.getEnabled())
            .kmsKey(cfnS3LoggingConfiguration.getKmsKey())
            .logLocation(cfnS3LoggingConfiguration.getLogLocation())
            .build();
  }

  private CustomerContentEncryptionConfiguration createSdkCustomerContentEncryptionConfigurationFromCfnConfiguration(
          software.amazon.athena.workgroup.CustomerContentEncryptionConfiguration customerContentEncryptionConfiguration) {
    return CustomerContentEncryptionConfiguration.builder()
            .kmsKey(customerContentEncryptionConfiguration.getKmsKey())
            .build();
  }

  private ManagedQueryResultsConfiguration createSdkManagedQueryResultsConfigurationFromCfnConfiguration(
          software.amazon.athena.workgroup.ManagedQueryResultsConfiguration managedQueryResultsConfiguration) {
    return ManagedQueryResultsConfiguration.builder()
            .enabled(managedQueryResultsConfiguration.getEnabled())
            .encryptionConfiguration(
                    managedQueryResultsConfiguration.getEncryptionConfiguration() != null ?
                    ManagedQueryResultsEncryptionConfiguration.builder()
                    .kmsKey(managedQueryResultsConfiguration.getEncryptionConfiguration().getKmsKey())
                    .build() : null)
            .build();
  }

  private AclConfiguration createSdkAclConfigurationFromCfnConfiguration(software.amazon.athena.workgroup.AclConfiguration aclConfiguration) {
    return AclConfiguration.builder()
            .s3AclOption(aclConfiguration.getS3AclOption())
            .build();
  }

  private ResultConfiguration createSdkResultConfigurationFromCfnConfiguration(software.amazon.athena.workgroup.ResultConfiguration resultConfiguration) {
    return ResultConfiguration.builder()
            .encryptionConfiguration(resultConfiguration.getEncryptionConfiguration() != null ?
                    createSdkEncryptionConfigurationFromCfnConfiguration(resultConfiguration.getEncryptionConfiguration()) : null)
            .outputLocation(resultConfiguration.getOutputLocation())
            .aclConfiguration(resultConfiguration.getAclConfiguration() != null ? createSdkAclConfigurationFromCfnConfiguration(resultConfiguration.getAclConfiguration()) : null)
            .expectedBucketOwner(resultConfiguration.getExpectedBucketOwner())
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
            .additionalConfiguration(configuration.getAdditionalConfiguration())
            .executionRole(configuration.getExecutionRole())
            .customerContentEncryptionConfiguration(configuration.getCustomerContentEncryptionConfiguration() != null ?
                    createSdkCustomerContentEncryptionConfigurationFromCfnConfiguration(configuration.getCustomerContentEncryptionConfiguration()) : null)
            .removeCustomerContentEncryptionConfiguration(configuration.getRemoveCustomerContentEncryptionConfiguration())
            .managedQueryResultsConfigurationUpdates(configuration.getManagedQueryResultsConfiguration() != null ?
                    createSdkManagedQueryResultsConfigurationUpdatesFromCfnConfiguration(configuration.getManagedQueryResultsConfiguration()) : null)
            .engineConfiguration(configuration.getEngineConfiguration() != null ?
                    createSdkEngineConfigurationFromCfnConfiguration(configuration.getEngineConfiguration()) : null)
            .monitoringConfiguration(configuration.getMonitoringConfiguration() != null ?
                    createSdkMonitoringConfigurationFromCfnConfiguration(configuration.getMonitoringConfiguration()) : null)
            .build();
  }

  private ResultConfigurationUpdates createSdkResultConfigurationUpdatesFromCfnConfigurationUpdate(software.amazon.athena.workgroup.ResultConfigurationUpdates resultConfigurationUpdate) {
    return ResultConfigurationUpdates.builder()
            .encryptionConfiguration(resultConfigurationUpdate.getEncryptionConfiguration() != null ?
                    createSdkEncryptionConfigurationFromCfnConfiguration(resultConfigurationUpdate.getEncryptionConfiguration()) : null)
            .outputLocation(resultConfigurationUpdate.getOutputLocation())
            .removeEncryptionConfiguration(resultConfigurationUpdate.getRemoveEncryptionConfiguration())
            .removeOutputLocation(resultConfigurationUpdate.getRemoveOutputLocation())
            .expectedBucketOwner(resultConfigurationUpdate.getExpectedBucketOwner())
            .removeExpectedBucketOwner(resultConfigurationUpdate.getRemoveExpectedBucketOwner())
            .aclConfiguration(resultConfigurationUpdate.getAclConfiguration() != null ?
                    createSdkAclConfigurationFromCfnConfiguration(resultConfigurationUpdate.getAclConfiguration()) : null)
            .removeAclConfiguration(resultConfigurationUpdate.getRemoveAclConfiguration())
            .build();
  }

  WorkGroupConfigurationUpdates createSdkConfigurationUpdatesFromCfnConfiguration(software.amazon.athena.workgroup.WorkGroupConfiguration requestedConfig) {
    WorkGroupConfigurationUpdates defaults = HandlerUtils.getDefaultWorkGroupConfiguration();
    WorkGroupConfigurationUpdates.Builder configUpdates = WorkGroupConfigurationUpdates.builder()
            .enforceWorkGroupConfiguration(requestedConfig.getEnforceWorkGroupConfiguration() != null ? requestedConfig.getEnforceWorkGroupConfiguration() : defaults.enforceWorkGroupConfiguration())
            .engineVersion(requestedConfig.getEngineVersion() != null ? createSdkEngineVersionFromCfnConfiguration(requestedConfig.getEngineVersion()) : EngineVersion.builder().selectedEngineVersion(defaults.engineVersion().selectedEngineVersion()).build())
            .publishCloudWatchMetricsEnabled(requestedConfig.getPublishCloudWatchMetricsEnabled() != null ? requestedConfig.getPublishCloudWatchMetricsEnabled() : defaults.publishCloudWatchMetricsEnabled())
            .requesterPaysEnabled(requestedConfig.getRequesterPaysEnabled() != null ? requestedConfig.getRequesterPaysEnabled() : defaults.requesterPaysEnabled())
            .resultConfigurationUpdates(createSdkResultConfigurationUpdatesFromCfnConfiguration(requestedConfig.getResultConfiguration()))
            .additionalConfiguration(requestedConfig.getAdditionalConfiguration())
            .managedQueryResultsConfigurationUpdates(requestedConfig.getManagedQueryResultsConfiguration() != null ?
                    createSdkManagedQueryResultsConfigurationUpdatesFromCfnConfiguration(requestedConfig.getManagedQueryResultsConfiguration()) :
                    defaults.managedQueryResultsConfigurationUpdates())
            .executionRole(requestedConfig.getExecutionRole())
            .engineConfiguration(requestedConfig.getEngineConfiguration() != null ?
                    createSdkEngineConfigurationFromCfnConfiguration(requestedConfig.getEngineConfiguration()) : null)
            .monitoringConfiguration(requestedConfig.getMonitoringConfiguration() != null ?
                    createSdkMonitoringConfigurationFromCfnConfiguration(requestedConfig.getMonitoringConfiguration()) : null);
    if (requestedConfig.getBytesScannedCutoffPerQuery() == null) {
      configUpdates.removeBytesScannedCutoffPerQuery(true);
    } else {
      configUpdates.bytesScannedCutoffPerQuery(requestedConfig.getBytesScannedCutoffPerQuery());
    }
    if (requestedConfig.getCustomerContentEncryptionConfiguration() == null) {
      configUpdates.removeCustomerContentEncryptionConfiguration(true);
    } else {
      configUpdates.customerContentEncryptionConfiguration(
              createSdkCustomerContentEncryptionConfigurationFromCfnConfiguration(requestedConfig.getCustomerContentEncryptionConfiguration()));
    }
    return configUpdates.build();
  }

  private ResultConfigurationUpdates createSdkResultConfigurationUpdatesFromCfnConfiguration(software.amazon.athena.workgroup.ResultConfiguration resultConfiguration) {
    ResultConfigurationUpdates.Builder updatesBuilder = ResultConfigurationUpdates.builder();
    if (resultConfiguration == null || resultConfiguration.getOutputLocation() == null) {
      updatesBuilder.removeOutputLocation(true);
      updatesBuilder.removeExpectedBucketOwner(true);
      updatesBuilder.removeAclConfiguration(true);
    } else {
      updatesBuilder.outputLocation(resultConfiguration.getOutputLocation());
      updatesBuilder.expectedBucketOwner(resultConfiguration.getExpectedBucketOwner());
      updatesBuilder.aclConfiguration(resultConfiguration.getAclConfiguration() != null ?
              createSdkAclConfigurationFromCfnConfiguration(resultConfiguration.getAclConfiguration()) : null);
    }
    if (resultConfiguration == null || resultConfiguration.getEncryptionConfiguration() == null) {
      updatesBuilder.removeEncryptionConfiguration(true);
    } else {
      updatesBuilder.encryptionConfiguration(createSdkEncryptionConfigurationFromCfnConfiguration(resultConfiguration.getEncryptionConfiguration()));
    }
    return updatesBuilder.build();
  }

  private ManagedQueryResultsConfigurationUpdates createSdkManagedQueryResultsConfigurationUpdatesFromCfnConfiguration(
          software.amazon.athena.workgroup.ManagedQueryResultsConfiguration managedQueryResultsConfiguration) {
    return ManagedQueryResultsConfigurationUpdates.builder()
            .enabled(managedQueryResultsConfiguration.getEnabled())
            .encryptionConfiguration(
                    managedQueryResultsConfiguration.getEncryptionConfiguration() != null ?
                    ManagedQueryResultsEncryptionConfiguration.builder()
                    .kmsKey(managedQueryResultsConfiguration.getEncryptionConfiguration().getKmsKey())
                    .build() : null)
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
            .additionalConfiguration(sdkConfiguration.additionalConfiguration())
            .executionRole(sdkConfiguration.executionRole())
            .customerContentEncryptionConfiguration(sdkConfiguration.customerContentEncryptionConfiguration() != null ?
                    createCfnCustomerContentEncryptionConfigurationFromSdkConfiguration(sdkConfiguration.customerContentEncryptionConfiguration()) : null)
            .managedQueryResultsConfiguration(sdkConfiguration.managedQueryResultsConfiguration() != null ?
                    createCfnManagedQueryResultsConfigurationFromSdkConfiguration(sdkConfiguration.managedQueryResultsConfiguration()) : null)
            .engineConfiguration(sdkConfiguration.engineConfiguration() != null ?
                    createCfnEngineConfigurationFromSdkConfiguration(sdkConfiguration.engineConfiguration()) : null)
            .monitoringConfiguration(sdkConfiguration.monitoringConfiguration() != null ?
                    createCfnMonitoringConfigurationFromSdkConfiguration(sdkConfiguration.monitoringConfiguration()) : null)
            .build();
  }

  private software.amazon.athena.workgroup.CustomerContentEncryptionConfiguration createCfnCustomerContentEncryptionConfigurationFromSdkConfiguration(
          CustomerContentEncryptionConfiguration customerContentEncryptionConfiguration) {
    return software.amazon.athena.workgroup.CustomerContentEncryptionConfiguration.builder()
            .kmsKey(customerContentEncryptionConfiguration.kmsKey())
            .build();
  }

  private software.amazon.athena.workgroup.ManagedQueryResultsConfiguration createCfnManagedQueryResultsConfigurationFromSdkConfiguration(
          ManagedQueryResultsConfiguration managedQueryResultsConfiguration) {
    return software.amazon.athena.workgroup.ManagedQueryResultsConfiguration.builder()
            .enabled(managedQueryResultsConfiguration.enabled())
            .encryptionConfiguration(managedQueryResultsConfiguration.encryptionConfiguration() != null ?
                    ManagedStorageEncryptionConfiguration.builder()
                                    .kmsKey(managedQueryResultsConfiguration.encryptionConfiguration().kmsKey())
                                    .build() : null)
            .build();
  }

  private software.amazon.athena.workgroup.EngineVersion createCfnEngineVersionFromSdkConfiguration(EngineVersion engineVersion) {
    return software.amazon.athena.workgroup.EngineVersion.builder()
            .selectedEngineVersion(engineVersion.selectedEngineVersion())
            .effectiveEngineVersion(engineVersion.effectiveEngineVersion())
            .build();
  }

  software.amazon.athena.workgroup.EngineConfiguration createCfnEngineConfigurationFromSdkConfiguration(EngineConfiguration sdkEngineConfiguration) {
    return software.amazon.athena.workgroup.EngineConfiguration.builder()
            .classifications(sdkEngineConfiguration.classifications() != null ?
                    createCfnClassificationsFromSdkClassifications(sdkEngineConfiguration.classifications()) : null)
            .build();
  }

  List<software.amazon.athena.workgroup.Classification> createCfnClassificationsFromSdkClassifications(List<Classification> sdkClassifications) {
    if (sdkClassifications == null) {
      return null;
    }
    List<software.amazon.athena.workgroup.Classification> cfnClassifications = new ArrayList<>();
    sdkClassifications.forEach(sdkClassification -> {
      cfnClassifications.add(createCfnClassificationFromSdkClassification(sdkClassification));
    });
    return cfnClassifications;
  }

  software.amazon.athena.workgroup.Classification createCfnClassificationFromSdkClassification(Classification sdkClassification) {
    return software.amazon.athena.workgroup.Classification.builder()
            .name(sdkClassification.name())
            .properties(sdkClassification.properties())
            .build();
  }

  software.amazon.athena.workgroup.MonitoringConfiguration createCfnMonitoringConfigurationFromSdkConfiguration(MonitoringConfiguration sdkMonitoringConfiguration) {
    return software.amazon.athena.workgroup.MonitoringConfiguration.builder()
            .cloudWatchLoggingConfiguration(sdkMonitoringConfiguration.cloudWatchLoggingConfiguration() != null ?
                    createCfnCloudWatchLoggingConfigurationFromSdkConfiguration(sdkMonitoringConfiguration.cloudWatchLoggingConfiguration()) : null)
            .managedLoggingConfiguration(sdkMonitoringConfiguration.managedLoggingConfiguration() != null ?
                    createCfnManagedLoggingConfigurationFromSdkConfiguration(sdkMonitoringConfiguration.managedLoggingConfiguration()) : null)
            .s3LoggingConfiguration(sdkMonitoringConfiguration.s3LoggingConfiguration() != null ?
                    createCfnS3LoggingConfigurationFromSdkConfiguration(sdkMonitoringConfiguration.s3LoggingConfiguration()) : null)
            .build();
  }

  software.amazon.athena.workgroup.CloudWatchLoggingConfiguration createCfnCloudWatchLoggingConfigurationFromSdkConfiguration(CloudWatchLoggingConfiguration sdkCloudWatchLoggingConfiguration) {
    return software.amazon.athena.workgroup.CloudWatchLoggingConfiguration.builder()
            .enabled(sdkCloudWatchLoggingConfiguration.enabled())
            .logGroup(sdkCloudWatchLoggingConfiguration.logGroup())
            .logStreamNamePrefix(sdkCloudWatchLoggingConfiguration.logStreamNamePrefix())
            .logTypes(convertSdkLogTypesToCfnLogTypes(sdkCloudWatchLoggingConfiguration.logTypes()))
            .build();
  }

  private Map<String, List<String>> convertCfnLogTypesToSdkLogTypes(Map<String, List<String>> cfnLogTypes) {
    return cfnLogTypes;
  }

  private Map<String, List<String>> convertSdkLogTypesToCfnLogTypes(Map<String, List<String>> sdkLogTypes) {
    return sdkLogTypes;
  }

  software.amazon.athena.workgroup.ManagedLoggingConfiguration createCfnManagedLoggingConfigurationFromSdkConfiguration(ManagedLoggingConfiguration sdkManagedLoggingConfiguration) {
    return software.amazon.athena.workgroup.ManagedLoggingConfiguration.builder()
            .enabled(sdkManagedLoggingConfiguration.enabled())
            .kmsKey(sdkManagedLoggingConfiguration.kmsKey())
            .build();
  }

  software.amazon.athena.workgroup.S3LoggingConfiguration createCfnS3LoggingConfigurationFromSdkConfiguration(S3LoggingConfiguration sdkS3LoggingConfiguration) {
    return software.amazon.athena.workgroup.S3LoggingConfiguration.builder()
            .enabled(sdkS3LoggingConfiguration.enabled())
            .kmsKey(sdkS3LoggingConfiguration.kmsKey())
            .logLocation(sdkS3LoggingConfiguration.logLocation())
            .build();
  }

  private software.amazon.athena.workgroup.ResultConfiguration createCfnResultConfigurationFromSdkConfiguration(ResultConfiguration resultConfiguration) {
    return software.amazon.athena.workgroup.ResultConfiguration.builder()
      .encryptionConfiguration(resultConfiguration.encryptionConfiguration() != null ? createCfnEncryptionConfigurationFromSdkConfiguration(resultConfiguration.encryptionConfiguration()) : null)
      .outputLocation(resultConfiguration.outputLocation())
      .expectedBucketOwner(resultConfiguration.expectedBucketOwner())
      .aclConfiguration(resultConfiguration.aclConfiguration() != null ? createCfnAclConfigurationFromSdkConfiguration(resultConfiguration.aclConfiguration()) : null)
      .build();
  }

  private software.amazon.athena.workgroup.AclConfiguration createCfnAclConfigurationFromSdkConfiguration(AclConfiguration aclConfiguration) {
    return software.amazon.athena.workgroup.AclConfiguration.builder()
            .s3AclOption(aclConfiguration.s3AclOptionAsString()).build();
  }

  private software.amazon.athena.workgroup.EncryptionConfiguration createCfnEncryptionConfigurationFromSdkConfiguration(EncryptionConfiguration encryptionConfiguration) {
    return software.amazon.athena.workgroup.EncryptionConfiguration.builder()
            .encryptionOption(encryptionConfiguration.encryptionOption().toString())
            .kmsKey(encryptionConfiguration.kmsKey())
            .build();
  }
}
