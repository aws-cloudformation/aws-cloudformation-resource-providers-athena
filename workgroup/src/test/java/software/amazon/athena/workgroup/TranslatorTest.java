package software.amazon.athena.workgroup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.model.AclConfiguration;
import software.amazon.awssdk.services.athena.model.CustomerContentEncryptionConfiguration;
import software.amazon.awssdk.services.athena.model.EngineVersion;
import software.amazon.awssdk.services.athena.model.EncryptionConfiguration;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.WorkGroupConfiguration;
import software.amazon.awssdk.services.athena.model.WorkGroupConfigurationUpdates;
import software.amazon.awssdk.services.athena.model.Classification;
import software.amazon.awssdk.services.athena.model.EngineConfiguration;
import software.amazon.awssdk.services.athena.model.MonitoringConfiguration;
import software.amazon.awssdk.services.athena.model.CloudWatchLoggingConfiguration;
import software.amazon.awssdk.services.athena.model.ManagedLoggingConfiguration;
import software.amazon.awssdk.services.athena.model.S3LoggingConfiguration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
public class TranslatorTest {
  @Test
  void testCreateSdkTagFromCfnTag() {
    Tag tag1 = Tag.builder()
            .key("Author").value("Jeff").build();
    Tag tag2 = Tag.builder()
            .key("Position").value("Founder").build();

    List<Tag> cfnTags = Arrays.asList(tag1, tag2);

    Map<String, String> stackTags = new HashMap<>();
    stackTags.put("Author", "Bezos");

    List<software.amazon.awssdk.services.athena.model.Tag> sdkTags =
      new Translator().createConsolidatedSdkTagsFromCfnTags(cfnTags, stackTags);

    assertThat(sdkTags.size()).isEqualTo(2);
    assertThat(sdkTags.get(0).value()).isEqualTo(cfnTags.get(0).getValue());
    assertThat(sdkTags.get(1).value()).isEqualTo(cfnTags.get(1).getValue());
  }

  @Test
  void testCreateSdkWorkgroupConfigurationFromCfnConfiguration() {
    software.amazon.athena.workgroup.EngineVersion sqlConfiguration = software.amazon.athena.workgroup.EngineVersion.builder()
            .selectedEngineVersion("AUTO")
            .effectiveEngineVersion("Athena engine version 2")
            .build();
    software.amazon.athena.workgroup.WorkGroupConfiguration cfnWorkGroupConfiguration =
            getBareMinCfnWorkGroupConfiguration()
                    .bytesScannedCutoffPerQuery(10_000_000_000L)
                    .engineVersion(sqlConfiguration)
                    .additionalConfiguration("{\"additionalConfig\": \"some_config\"}")
                    .build();

    WorkGroupConfiguration sdkWorkGroupConfiguration =
      new Translator().createSdkWorkgroupConfigurationFromCfnConfiguration(cfnWorkGroupConfiguration);

    assertThat(cfnWorkGroupConfiguration.getEnforceWorkGroupConfiguration()).isEqualTo(sdkWorkGroupConfiguration.enforceWorkGroupConfiguration());
    assertThat(cfnWorkGroupConfiguration.getPublishCloudWatchMetricsEnabled()).isEqualTo(sdkWorkGroupConfiguration.publishCloudWatchMetricsEnabled());
    assertThat(cfnWorkGroupConfiguration.getRequesterPaysEnabled()).isEqualTo(sdkWorkGroupConfiguration.requesterPaysEnabled());
    assertThat(cfnWorkGroupConfiguration.getBytesScannedCutoffPerQuery()).isEqualTo(sdkWorkGroupConfiguration.bytesScannedCutoffPerQuery());
    assertThat(cfnWorkGroupConfiguration.getResultConfiguration().getOutputLocation()).isEqualTo(sdkWorkGroupConfiguration.resultConfiguration().outputLocation());
    assertThat(cfnWorkGroupConfiguration.getResultConfiguration().getEncryptionConfiguration().getEncryptionOption())
      .isEqualTo(sdkWorkGroupConfiguration.resultConfiguration().encryptionConfiguration().encryptionOption().toString());
    assertThat(cfnWorkGroupConfiguration.getResultConfiguration().getExpectedBucketOwner()).isEqualTo(sdkWorkGroupConfiguration.resultConfiguration().expectedBucketOwner());
    assertThat(cfnWorkGroupConfiguration.getResultConfiguration().getAclConfiguration().getS3AclOption()).isEqualTo(sdkWorkGroupConfiguration.resultConfiguration().aclConfiguration().s3AclOptionAsString());
    assertThat(cfnWorkGroupConfiguration.getEngineVersion().getSelectedEngineVersion())
            .isEqualTo(sdkWorkGroupConfiguration.engineVersion().selectedEngineVersion());
    assertThat(cfnWorkGroupConfiguration.getEngineVersion().getEffectiveEngineVersion())
            .isEqualTo(sdkWorkGroupConfiguration.engineVersion().effectiveEngineVersion());
  }

  @Test
  void testCreateSdkWorkgroupConfigurationFromCfnConfigurationForApacheSparkWorkgroup() {
    software.amazon.athena.workgroup.EngineVersion sparkEngine = software.amazon.athena.workgroup.EngineVersion.builder()
            .selectedEngineVersion("AUTO")
            .effectiveEngineVersion("PySpark engine version 3")
            .build();
    software.amazon.athena.workgroup.WorkGroupConfiguration cfnWorkGroupConfiguration =
            getBareMinCfnWorkGroupConfiguration()
                    .engineVersion(sparkEngine)
                    .executionRole("arn:aws:iam::123456789012:role/service-role/fake-execution-role")
                    .customerContentEncryptionConfiguration(software.amazon.athena.workgroup.CustomerContentEncryptionConfiguration.builder()
                            .kmsKey("arn:aws:kms:us-east-1:123456789012:key/fake-kms-key-id").build())
                    .build();

    WorkGroupConfiguration sdkWorkGroupConfiguration =
            new Translator().createSdkWorkgroupConfigurationFromCfnConfiguration(cfnWorkGroupConfiguration);

    assertThat(cfnWorkGroupConfiguration.getEnforceWorkGroupConfiguration()).isEqualTo(sdkWorkGroupConfiguration.enforceWorkGroupConfiguration());
    assertThat(cfnWorkGroupConfiguration.getPublishCloudWatchMetricsEnabled()).isEqualTo(sdkWorkGroupConfiguration.publishCloudWatchMetricsEnabled());
    assertThat(cfnWorkGroupConfiguration.getRequesterPaysEnabled()).isEqualTo(sdkWorkGroupConfiguration.requesterPaysEnabled());
    assertThat(cfnWorkGroupConfiguration.getBytesScannedCutoffPerQuery()).isNull();
    assertThat(cfnWorkGroupConfiguration.getResultConfiguration().getOutputLocation()).isEqualTo(sdkWorkGroupConfiguration.resultConfiguration().outputLocation());
    assertThat(cfnWorkGroupConfiguration.getResultConfiguration().getEncryptionConfiguration().getEncryptionOption())
            .isEqualTo(sdkWorkGroupConfiguration.resultConfiguration().encryptionConfiguration().encryptionOption().toString());
    assertThat(cfnWorkGroupConfiguration.getResultConfiguration().getExpectedBucketOwner()).isEqualTo(sdkWorkGroupConfiguration.resultConfiguration().expectedBucketOwner());
    assertThat(cfnWorkGroupConfiguration.getResultConfiguration().getAclConfiguration().getS3AclOption()).isEqualTo(sdkWorkGroupConfiguration.resultConfiguration().aclConfiguration().s3AclOptionAsString());
    assertThat(cfnWorkGroupConfiguration.getEngineVersion().getSelectedEngineVersion())
            .isEqualTo(sdkWorkGroupConfiguration.engineVersion().selectedEngineVersion());
    assertThat(cfnWorkGroupConfiguration.getEngineVersion().getEffectiveEngineVersion())
            .isEqualTo(sdkWorkGroupConfiguration.engineVersion().effectiveEngineVersion());
    assertThat(cfnWorkGroupConfiguration.getExecutionRole()).isEqualTo(sdkWorkGroupConfiguration.executionRole());
    assertThat(cfnWorkGroupConfiguration.getCustomerContentEncryptionConfiguration().getKmsKey())
            .isEqualTo(sdkWorkGroupConfiguration.customerContentEncryptionConfiguration().kmsKey());
  }

  @Test
  void testCreateSdkWorkgroupConfigurationFromCfnConfigurationForManagedStorageWorkgroup() {
    software.amazon.athena.workgroup.EngineVersion sqlConfiguration = software.amazon.athena.workgroup.EngineVersion.builder()
            .selectedEngineVersion("AUTO")
            .effectiveEngineVersion("Athena engine version 2")
            .build();
    software.amazon.athena.workgroup.WorkGroupConfiguration cfnWorkGroupConfiguration =
            getBareMinCfnWorkGroupConfiguration()
                    .bytesScannedCutoffPerQuery(10_000_000_000L)
                    .engineVersion(sqlConfiguration)
                    .managedQueryResultsConfiguration(ManagedQueryResultsConfiguration.builder()
                            .enabled(true)
                            .encryptionConfiguration(ManagedStorageEncryptionConfiguration.builder()
                                    .kmsKey("TestKey")
                                    .build())
                            .build())
                    .build();
    WorkGroupConfiguration sdkWorkGroupConfiguration =
            new Translator().createSdkWorkgroupConfigurationFromCfnConfiguration(cfnWorkGroupConfiguration);
    assertThat(cfnWorkGroupConfiguration.getEnforceWorkGroupConfiguration()).isEqualTo(sdkWorkGroupConfiguration.enforceWorkGroupConfiguration());
    assertThat(cfnWorkGroupConfiguration.getPublishCloudWatchMetricsEnabled()).isEqualTo(sdkWorkGroupConfiguration.publishCloudWatchMetricsEnabled());
    assertThat(cfnWorkGroupConfiguration.getRequesterPaysEnabled()).isEqualTo(sdkWorkGroupConfiguration.requesterPaysEnabled());
    assertThat(cfnWorkGroupConfiguration.getEngineVersion().getSelectedEngineVersion())
            .isEqualTo(sdkWorkGroupConfiguration.engineVersion().selectedEngineVersion());
    assertThat(cfnWorkGroupConfiguration.getEngineVersion().getEffectiveEngineVersion())
            .isEqualTo(sdkWorkGroupConfiguration.engineVersion().effectiveEngineVersion());
    assertThat(cfnWorkGroupConfiguration.getExecutionRole()).isEqualTo(sdkWorkGroupConfiguration.executionRole());
    assertThat(cfnWorkGroupConfiguration.getManagedQueryResultsConfiguration().getEnabled())
            .isEqualTo(sdkWorkGroupConfiguration.managedQueryResultsConfiguration().enabled());
    assertThat(cfnWorkGroupConfiguration.getManagedQueryResultsConfiguration().getEncryptionConfiguration().getKmsKey())
            .isEqualTo(sdkWorkGroupConfiguration.managedQueryResultsConfiguration().encryptionConfiguration().kmsKey());
  }

  @Test
  void testCreateSdkWorkgroupConfigurationFromCfnConfigurationWithResultConfigurationNullable() {
    software.amazon.athena.workgroup.WorkGroupConfiguration cfnWorkGroupConfiguration = software.amazon.athena.workgroup.WorkGroupConfiguration.builder()
            .enforceWorkGroupConfiguration(true)
            .bytesScannedCutoffPerQuery(10_000_000_000L)
            .publishCloudWatchMetricsEnabled(true)
            .requesterPaysEnabled(false)
            .resultConfiguration(null)
            .engineVersion(null)
            .customerContentEncryptionConfiguration(null)
            .build();

    WorkGroupConfiguration sdkWorkGroupConfiguration =
            new Translator().createSdkWorkgroupConfigurationFromCfnConfiguration(cfnWorkGroupConfiguration);

    assertThat(cfnWorkGroupConfiguration.getEnforceWorkGroupConfiguration()).isEqualTo(sdkWorkGroupConfiguration.enforceWorkGroupConfiguration());
    assertThat(cfnWorkGroupConfiguration.getBytesScannedCutoffPerQuery()).isEqualTo(sdkWorkGroupConfiguration.bytesScannedCutoffPerQuery());
    assertThat(cfnWorkGroupConfiguration.getPublishCloudWatchMetricsEnabled()).isEqualTo(sdkWorkGroupConfiguration.publishCloudWatchMetricsEnabled());
    assertThat(cfnWorkGroupConfiguration.getRequesterPaysEnabled()).isEqualTo(sdkWorkGroupConfiguration.requesterPaysEnabled());
    assertThat(cfnWorkGroupConfiguration.getEngineVersion()).isEqualTo(sdkWorkGroupConfiguration.engineVersion());
    assertThat(cfnWorkGroupConfiguration.getCustomerContentEncryptionConfiguration()).isEqualTo(sdkWorkGroupConfiguration.customerContentEncryptionConfiguration());
  }

  @Test
  void testCreateSdkConfigurationUpdatesFromCfnConfigurationUpdates() {
    software.amazon.athena.workgroup.EngineVersion engineVersion = software.amazon.athena.workgroup.EngineVersion.builder()
            .selectedEngineVersion("AUTO")
            .effectiveEngineVersion("Athena engine version 2")
            .build();
    software.amazon.athena.workgroup.WorkGroupConfigurationUpdates cfnWorkGroupConfiguration =
            software.amazon.athena.workgroup.WorkGroupConfigurationUpdates.builder()
            .enforceWorkGroupConfiguration(true)
            .bytesScannedCutoffPerQuery(10_000_000_000L)
            .publishCloudWatchMetricsEnabled(true)
            .removeBytesScannedCutoffPerQuery(true)
            .requesterPaysEnabled(false)
            .resultConfigurationUpdates(software.amazon.athena.workgroup.ResultConfigurationUpdates.builder()
                    .outputLocation("s3://test/")
                    .encryptionConfiguration(software.amazon.athena.workgroup.EncryptionConfiguration.builder()
                            .encryptionOption("CSE_KMS")
                            .kmsKey("some_key")
                            .build())
                    .expectedBucketOwner("123456789012")
                    .aclConfiguration(software.amazon.athena.workgroup.AclConfiguration.builder().s3AclOption("BUCKET_OWNER_FULL_CONTROL").build())
                    .removeOutputLocation(true)
                    .removeEncryptionConfiguration(true)
                    .removeExpectedBucketOwner(true)
                    .removeAclConfiguration(true)
                    .build())
            .engineVersion(engineVersion)
            .additionalConfiguration("{\"additionalConfig\": \"some_config\"}")
            .executionRole("arn:aws:iam::123456789012:role/service-role/fake-execution-role")
            .customerContentEncryptionConfiguration(software.amazon.athena.workgroup.CustomerContentEncryptionConfiguration.builder()
                    .kmsKey("arn:aws:kms:us-east-1:123456789012:key/fake-kms-key-id").build())
            .removeCustomerContentEncryptionConfiguration(true)
            .build();

    WorkGroupConfigurationUpdates sdkWorkGroupConfigurationUpdates =
            new Translator().createSdkConfigurationUpdatesFromCfnConfigurationUpdates(cfnWorkGroupConfiguration);

    assertThat(cfnWorkGroupConfiguration.getEnforceWorkGroupConfiguration()).isEqualTo(sdkWorkGroupConfigurationUpdates.enforceWorkGroupConfiguration());
    assertThat(cfnWorkGroupConfiguration.getPublishCloudWatchMetricsEnabled()).isEqualTo(sdkWorkGroupConfigurationUpdates.publishCloudWatchMetricsEnabled());
    assertThat(cfnWorkGroupConfiguration.getRequesterPaysEnabled()).isEqualTo(sdkWorkGroupConfigurationUpdates.requesterPaysEnabled());
    assertThat(cfnWorkGroupConfiguration.getRemoveBytesScannedCutoffPerQuery()).isEqualTo(sdkWorkGroupConfigurationUpdates.removeBytesScannedCutoffPerQuery());
    assertThat(cfnWorkGroupConfiguration.getResultConfigurationUpdates().getRemoveOutputLocation())
            .isEqualTo(sdkWorkGroupConfigurationUpdates.resultConfigurationUpdates().removeOutputLocation());
    assertThat(cfnWorkGroupConfiguration.getResultConfigurationUpdates().getRemoveEncryptionConfiguration())
            .isEqualTo(sdkWorkGroupConfigurationUpdates.resultConfigurationUpdates().removeEncryptionConfiguration());
    assertThat(cfnWorkGroupConfiguration.getResultConfigurationUpdates().getRemoveExpectedBucketOwner())
            .isEqualTo(sdkWorkGroupConfigurationUpdates.resultConfigurationUpdates().removeExpectedBucketOwner());
    assertThat(cfnWorkGroupConfiguration.getResultConfigurationUpdates().getRemoveAclConfiguration())
            .isEqualTo(sdkWorkGroupConfigurationUpdates.resultConfigurationUpdates().removeAclConfiguration());
    assertThat(cfnWorkGroupConfiguration.getEngineVersion().getSelectedEngineVersion())
            .isEqualTo(sdkWorkGroupConfigurationUpdates.engineVersion().selectedEngineVersion());
    assertThat(cfnWorkGroupConfiguration.getEngineVersion().getEffectiveEngineVersion())
            .isEqualTo(sdkWorkGroupConfigurationUpdates.engineVersion().effectiveEngineVersion());
    assertThat(cfnWorkGroupConfiguration.getAdditionalConfiguration()).isEqualTo(sdkWorkGroupConfigurationUpdates.additionalConfiguration());
    assertThat(cfnWorkGroupConfiguration.getExecutionRole()).isEqualTo(sdkWorkGroupConfigurationUpdates.executionRole());
    assertThat(cfnWorkGroupConfiguration.getCustomerContentEncryptionConfiguration().getKmsKey())
            .isEqualTo(sdkWorkGroupConfigurationUpdates.customerContentEncryptionConfiguration().kmsKey());
    assertThat(cfnWorkGroupConfiguration.getRemoveCustomerContentEncryptionConfiguration())
            .isEqualTo(sdkWorkGroupConfigurationUpdates.removeCustomerContentEncryptionConfiguration());
  }

  @Test
  void testCreateSdkConfigurationUpdatesFromCfnConfigurationUpdatesForManagedStorageWorkgroup() {
    software.amazon.athena.workgroup.EngineVersion engineVersion = software.amazon.athena.workgroup.EngineVersion.builder()
            .selectedEngineVersion("AUTO")
            .effectiveEngineVersion("Athena engine version 2")
            .build();
    software.amazon.athena.workgroup.WorkGroupConfigurationUpdates cfnWorkGroupConfiguration =
            software.amazon.athena.workgroup.WorkGroupConfigurationUpdates.builder()
                    .bytesScannedCutoffPerQuery(10_000_000_000L)
                    .managedQueryResultsConfiguration(ManagedQueryResultsConfiguration.builder()
                            .enabled(true)
                            .encryptionConfiguration(ManagedStorageEncryptionConfiguration.builder()
                                    .kmsKey("testKey")
                                    .build())
                            .build())
                    .engineVersion(engineVersion)
                    .removeCustomerContentEncryptionConfiguration(true)
                    .build();
    WorkGroupConfigurationUpdates sdkWorkGroupConfigurationUpdates =
            new Translator().createSdkConfigurationUpdatesFromCfnConfigurationUpdates(cfnWorkGroupConfiguration);
    assertThat(cfnWorkGroupConfiguration.getManagedQueryResultsConfiguration().getEnabled())
            .isEqualTo(sdkWorkGroupConfigurationUpdates.managedQueryResultsConfigurationUpdates().enabled());
    assertThat(cfnWorkGroupConfiguration.getManagedQueryResultsConfiguration().getEncryptionConfiguration().getKmsKey())
            .isEqualTo(sdkWorkGroupConfigurationUpdates.managedQueryResultsConfigurationUpdates().encryptionConfiguration().kmsKey());
  }

  @Test
  void testCreateSdkConfigurationUpdatesFromCfnConfigurationUpdatesWithResultUpdatesNullable() {
    software.amazon.athena.workgroup.WorkGroupConfigurationUpdates cfnWorkGroupConfiguration = software.amazon.athena.workgroup.WorkGroupConfigurationUpdates.builder()
            .enforceWorkGroupConfiguration(true)
            .bytesScannedCutoffPerQuery(10_000_000_000L)
            .publishCloudWatchMetricsEnabled(true)
            .removeBytesScannedCutoffPerQuery(true)
            .requesterPaysEnabled(false)
            .resultConfigurationUpdates(null)
            .engineVersion(null)
            .build();

    WorkGroupConfigurationUpdates sdkWorkGroupConfigurationUpdates =
            new Translator().createSdkConfigurationUpdatesFromCfnConfigurationUpdates(cfnWorkGroupConfiguration);

    assertThat(cfnWorkGroupConfiguration.getEnforceWorkGroupConfiguration()).isEqualTo(sdkWorkGroupConfigurationUpdates.enforceWorkGroupConfiguration());
    assertThat(cfnWorkGroupConfiguration.getPublishCloudWatchMetricsEnabled()).isEqualTo(sdkWorkGroupConfigurationUpdates.publishCloudWatchMetricsEnabled());
    assertThat(cfnWorkGroupConfiguration.getRequesterPaysEnabled()).isEqualTo(sdkWorkGroupConfigurationUpdates.requesterPaysEnabled());
    assertThat(cfnWorkGroupConfiguration.getRemoveBytesScannedCutoffPerQuery()).isEqualTo(sdkWorkGroupConfigurationUpdates.removeBytesScannedCutoffPerQuery());
    assertThat(cfnWorkGroupConfiguration.getEngineVersion()).isEqualTo(sdkWorkGroupConfigurationUpdates.engineVersion());
  }


  @Test
  void testCreateCfnWorkgroupConfigurationFromSdkConfigurationSQL() {
    EngineVersion sqlEngine = EngineVersion.builder()
            .selectedEngineVersion("AUTO")
            .effectiveEngineVersion("Athena engine version 1")
            .build();
    WorkGroupConfiguration sdkWorkGroupConfiguration = getBareMinSdkWorkGroupConfigBuilder()
            .bytesScannedCutoffPerQuery(10_000_000_000L)
            .engineVersion(sqlEngine)
            .build();

    software.amazon.athena.workgroup.WorkGroupConfiguration cfnWorkGroupConfiguration = new Translator().createCfnWorkgroupConfigurationFromSdkConfiguration(sdkWorkGroupConfiguration);
    assertThat(cfnWorkGroupConfiguration.getEnforceWorkGroupConfiguration()).isEqualTo(sdkWorkGroupConfiguration.enforceWorkGroupConfiguration());
    assertThat(cfnWorkGroupConfiguration.getBytesScannedCutoffPerQuery()).isEqualTo(sdkWorkGroupConfiguration.bytesScannedCutoffPerQuery());
    assertThat(cfnWorkGroupConfiguration.getPublishCloudWatchMetricsEnabled()).isEqualTo(sdkWorkGroupConfiguration.publishCloudWatchMetricsEnabled());
    assertThat(cfnWorkGroupConfiguration.getRequesterPaysEnabled()).isEqualTo(sdkWorkGroupConfiguration.requesterPaysEnabled());
    assertThat(cfnWorkGroupConfiguration.getResultConfiguration().getOutputLocation()).isEqualTo(sdkWorkGroupConfiguration.resultConfiguration().outputLocation());
    assertThat(cfnWorkGroupConfiguration.getResultConfiguration().getEncryptionConfiguration().getEncryptionOption()).isEqualTo(sdkWorkGroupConfiguration.resultConfiguration().encryptionConfiguration().encryptionOption().toString());
    assertThat(cfnWorkGroupConfiguration.getEngineVersion().getSelectedEngineVersion())
            .isEqualTo(sdkWorkGroupConfiguration.engineVersion().selectedEngineVersion());
    assertThat(cfnWorkGroupConfiguration.getEngineVersion().getEffectiveEngineVersion())
            .isEqualTo(sdkWorkGroupConfiguration.engineVersion().effectiveEngineVersion());
  }

  @Test
  void testCreateCfnWorkgroupConfigurationFromSdkConfigurationSpark() {
    EngineVersion sparkEngine = EngineVersion.builder()
            .selectedEngineVersion("AUTO")
            .effectiveEngineVersion("PySpark engine version 3")
            .build();
    WorkGroupConfiguration sdkWorkGroupConfiguration = getBareMinSdkWorkGroupConfigBuilder()
            .engineVersion(sparkEngine)
            .additionalConfiguration("{\"additionalConfig\": \"some_config\"}")
            .executionRole("arn:aws:iam::123456789012:role/service-role/fake-execution-role")
            .customerContentEncryptionConfiguration(CustomerContentEncryptionConfiguration.builder()
                    .kmsKey("arn:aws:kms:us-east-1:123456789012:key/fake-kms-key-id").build())
            .build();

    software.amazon.athena.workgroup.WorkGroupConfiguration cfnWorkGroupConfiguration = new Translator().createCfnWorkgroupConfigurationFromSdkConfiguration(sdkWorkGroupConfiguration);

    assertThat(cfnWorkGroupConfiguration.getEnforceWorkGroupConfiguration()).isEqualTo(sdkWorkGroupConfiguration.enforceWorkGroupConfiguration());
    assertThat(cfnWorkGroupConfiguration.getBytesScannedCutoffPerQuery()).isNull();
    assertThat(cfnWorkGroupConfiguration.getPublishCloudWatchMetricsEnabled()).isEqualTo(sdkWorkGroupConfiguration.publishCloudWatchMetricsEnabled());
    assertThat(cfnWorkGroupConfiguration.getRequesterPaysEnabled()).isEqualTo(sdkWorkGroupConfiguration.requesterPaysEnabled());
    assertThat(cfnWorkGroupConfiguration.getResultConfiguration().getOutputLocation()).isEqualTo(sdkWorkGroupConfiguration.resultConfiguration().outputLocation());
    assertThat(cfnWorkGroupConfiguration.getResultConfiguration().getEncryptionConfiguration().getEncryptionOption()).isEqualTo(sdkWorkGroupConfiguration.resultConfiguration().encryptionConfiguration().encryptionOption().toString());
    assertThat(cfnWorkGroupConfiguration.getEngineVersion().getSelectedEngineVersion())
            .isEqualTo(sdkWorkGroupConfiguration.engineVersion().selectedEngineVersion());
    assertThat(cfnWorkGroupConfiguration.getEngineVersion().getEffectiveEngineVersion())
            .isEqualTo(sdkWorkGroupConfiguration.engineVersion().effectiveEngineVersion());

    assertThat(cfnWorkGroupConfiguration.getExecutionRole())
            .isEqualTo(sdkWorkGroupConfiguration.executionRole());
    assertThat(cfnWorkGroupConfiguration.getCustomerContentEncryptionConfiguration().getKmsKey())
            .isEqualTo(sdkWorkGroupConfiguration.customerContentEncryptionConfiguration().kmsKey());
  }

  @Test
  void testCreateCfnWorkgroupConfigurationFromSdkConfigurationResultConfigurationNullable() {
    WorkGroupConfiguration sdkWorkGroupConfiguration = WorkGroupConfiguration.builder()
            .enforceWorkGroupConfiguration(true)
            .bytesScannedCutoffPerQuery(10_000_000_000L)
            .publishCloudWatchMetricsEnabled(true)
            .requesterPaysEnabled(false)
            .build();

    software.amazon.athena.workgroup.WorkGroupConfiguration cfnWorkGroupConfiguration = new Translator().createCfnWorkgroupConfigurationFromSdkConfiguration(sdkWorkGroupConfiguration);

    assertThat(cfnWorkGroupConfiguration.getEnforceWorkGroupConfiguration()).isEqualTo(sdkWorkGroupConfiguration.enforceWorkGroupConfiguration());
    assertThat(cfnWorkGroupConfiguration.getBytesScannedCutoffPerQuery()).isEqualTo(sdkWorkGroupConfiguration.bytesScannedCutoffPerQuery());
    assertThat(cfnWorkGroupConfiguration.getPublishCloudWatchMetricsEnabled()).isEqualTo(sdkWorkGroupConfiguration.publishCloudWatchMetricsEnabled());
    assertThat(cfnWorkGroupConfiguration.getRequesterPaysEnabled()).isEqualTo(sdkWorkGroupConfiguration.requesterPaysEnabled());
    assertThat(cfnWorkGroupConfiguration.getEngineVersion()).isEqualTo(sdkWorkGroupConfiguration.engineVersion());
  }

  @Test
  void testCreateSdkClassificationFromCfnClassification() {
    Map<String, String> properties = new HashMap<>();
    properties.put("spark.sql.adaptive.enabled", "true");
    properties.put("spark.sql.adaptive.coalescePartitions.enabled", "true");
    
    software.amazon.athena.workgroup.Classification cfnClassification = 
            software.amazon.athena.workgroup.Classification.builder()
                    .name("spark-defaults")
                    .properties(properties)
                    .build();

    Translator translator = new Translator();
    Classification sdkClassification = translator.createSdkClassificationFromCfnClassification(cfnClassification);

    assertThat(sdkClassification.name()).isEqualTo(cfnClassification.getName());
    assertThat(sdkClassification.properties()).isEqualTo(cfnClassification.getProperties());
  }

  @Test
  void testCreateCfnClassificationFromSdkClassification() {
    Map<String, String> properties = new HashMap<>();
    properties.put("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory");
    
    Classification sdkClassification = Classification.builder()
            .name("hive-site")
            .properties(properties)
            .build();

    Translator translator = new Translator();
    software.amazon.athena.workgroup.Classification cfnClassification = translator.createCfnClassificationFromSdkClassification(sdkClassification);

    assertThat(cfnClassification.getName()).isEqualTo(sdkClassification.name());
    assertThat(cfnClassification.getProperties()).isEqualTo(sdkClassification.properties());
  }

  @Test
  void testCreateSdkClassificationsFromCfnClassifications() {
    Map<String, String> sparkProperties = new HashMap<>();
    sparkProperties.put("spark.sql.adaptive.enabled", "true");
    
    Map<String, String> hiveProperties = new HashMap<>();
    hiveProperties.put("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory");
    
    List<software.amazon.athena.workgroup.Classification> cfnClassifications = Arrays.asList(
            software.amazon.athena.workgroup.Classification.builder()
                    .name("spark-defaults")
                    .properties(sparkProperties)
                    .build(),
            software.amazon.athena.workgroup.Classification.builder()
                    .name("hive-site")
                    .properties(hiveProperties)
                    .build()
    );

    Translator translator = new Translator();
    List<Classification> sdkClassifications = translator.createSdkClassificationsFromCfnClassifications(cfnClassifications);

    assertThat(sdkClassifications).hasSize(2);
    assertThat(sdkClassifications.get(0).name()).isEqualTo("spark-defaults");
    assertThat(sdkClassifications.get(0).properties()).isEqualTo(sparkProperties);
    assertThat(sdkClassifications.get(1).name()).isEqualTo("hive-site");
    assertThat(sdkClassifications.get(1).properties()).isEqualTo(hiveProperties);
  }

  @Test
  void testCreateCfnClassificationsFromSdkClassifications() {
    Map<String, String> sparkProperties = new HashMap<>();
    sparkProperties.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    
    Map<String, String> hiveProperties = new HashMap<>();
    hiveProperties.put("javax.jdo.option.ConnectionURL", "jdbc:mysql://localhost/metastore");
    
    List<Classification> sdkClassifications = Arrays.asList(
            Classification.builder()
                    .name("spark-defaults")
                    .properties(sparkProperties)
                    .build(),
            Classification.builder()
                    .name("hive-site")
                    .properties(hiveProperties)
                    .build()
    );

    Translator translator = new Translator();
    List<software.amazon.athena.workgroup.Classification> cfnClassifications = translator.createCfnClassificationsFromSdkClassifications(sdkClassifications);

    assertThat(cfnClassifications).hasSize(2);
    assertThat(cfnClassifications.get(0).getName()).isEqualTo("spark-defaults");
    assertThat(cfnClassifications.get(0).getProperties()).isEqualTo(sparkProperties);
    assertThat(cfnClassifications.get(1).getName()).isEqualTo("hive-site");
    assertThat(cfnClassifications.get(1).getProperties()).isEqualTo(hiveProperties);
  }

  @Test
  void testCreateSdkClassificationsFromCfnClassificationsWithNull() {
    Translator translator = new Translator();
    List<Classification> result = translator.createSdkClassificationsFromCfnClassifications(null);
    
    assertThat(result).isNull();
  }

  @Test
  void testCreateCfnClassificationsFromSdkClassificationsWithNull() {
    Translator translator = new Translator();
    List<software.amazon.athena.workgroup.Classification> result = translator.createCfnClassificationsFromSdkClassifications(null);
    
    assertThat(result).isNull();
  }

  @Test
  void testCreateSdkClassificationsFromCfnClassificationsWithEmptyList() {
    Translator translator = new Translator();
    List<Classification> result = translator.createSdkClassificationsFromCfnClassifications(Arrays.asList());
    
    assertThat(result).isEmpty();
  }

  @Test
  void testCreateCfnClassificationsFromSdkClassificationsWithEmptyList() {
    Translator translator = new Translator();
    List<software.amazon.athena.workgroup.Classification> result = translator.createCfnClassificationsFromSdkClassifications(Arrays.asList());
    
    assertThat(result).isEmpty();
  }

  @Test
  void testCreateSdkClassificationFromCfnClassificationWithNullProperties() {
    software.amazon.athena.workgroup.Classification cfnClassification = 
            software.amazon.athena.workgroup.Classification.builder()
                    .name("spark-defaults")
                    .properties(null)
                    .build();

    Translator translator = new Translator();
    Classification sdkClassification = translator.createSdkClassificationFromCfnClassification(cfnClassification);

    assertThat(sdkClassification.name()).isEqualTo("spark-defaults");
    // AWS SDK v2 might return an empty map instead of null when properties is not set
    assertThat(sdkClassification.properties()).isNullOrEmpty();
  }

  @Test
  void testCreateCfnClassificationFromSdkClassificationWithNullProperties() {
    Classification sdkClassification = Classification.builder()
            .name("hive-site")
            .properties(null)
            .build();

    Translator translator = new Translator();
    software.amazon.athena.workgroup.Classification cfnClassification = translator.createCfnClassificationFromSdkClassification(sdkClassification);

    assertThat(cfnClassification.getName()).isEqualTo("hive-site");
    // AWS SDK v2 might return an empty map instead of null when properties is not set
    assertThat(cfnClassification.getProperties()).isNullOrEmpty();
  }

  @Test
  void testCreateSdkEngineConfigurationFromCfnConfiguration() {
    Map<String, String> sparkProperties = new HashMap<>();
    sparkProperties.put("spark.sql.adaptive.enabled", "true");
    sparkProperties.put("spark.sql.adaptive.coalescePartitions.enabled", "true");
    
    software.amazon.athena.workgroup.Classification cfnClassification = 
            software.amazon.athena.workgroup.Classification.builder()
                    .name("spark-defaults")
                    .properties(sparkProperties)
                    .build();
    
    software.amazon.athena.workgroup.EngineConfiguration cfnEngineConfig = 
            software.amazon.athena.workgroup.EngineConfiguration.builder()
                    .classifications(Arrays.asList(cfnClassification))
                    .build();

    Translator translator = new Translator();
    EngineConfiguration sdkEngineConfig = translator.createSdkEngineConfigurationFromCfnConfiguration(cfnEngineConfig);

    assertThat(sdkEngineConfig.classifications()).hasSize(1);
    assertThat(sdkEngineConfig.classifications().get(0).name()).isEqualTo("spark-defaults");
    assertThat(sdkEngineConfig.classifications().get(0).properties()).isEqualTo(sparkProperties);
  }

  @Test
  void testCreateSdkEngineConfigurationFromCfnConfigurationWithNullClassifications() {
    software.amazon.athena.workgroup.EngineConfiguration cfnEngineConfig = 
            software.amazon.athena.workgroup.EngineConfiguration.builder()
                    .classifications(null)
                    .build();

    Translator translator = new Translator();
    EngineConfiguration sdkEngineConfig = translator.createSdkEngineConfigurationFromCfnConfiguration(cfnEngineConfig);

    // AWS SDK v2 might return an empty list instead of null when classifications is not set
    assertThat(sdkEngineConfig.classifications()).isNullOrEmpty();
  }

  @Test
  void testCreateSdkMonitoringConfigurationFromCfnConfiguration() {
    software.amazon.athena.workgroup.CloudWatchLoggingConfiguration cfnCloudWatchConfig = 
            software.amazon.athena.workgroup.CloudWatchLoggingConfiguration.builder()
                    .enabled(true)
                    .logGroup("/aws/athena/workgroup/test")
                    .build();
    
    software.amazon.athena.workgroup.ManagedLoggingConfiguration cfnManagedConfig = 
            software.amazon.athena.workgroup.ManagedLoggingConfiguration.builder()
                    .enabled(true)
                    .build();
    
    software.amazon.athena.workgroup.S3LoggingConfiguration cfnS3Config = 
            software.amazon.athena.workgroup.S3LoggingConfiguration.builder()
                    .enabled(true)
                    .logLocation("s3://test-logs/")
                    .build();
    
    software.amazon.athena.workgroup.MonitoringConfiguration cfnMonitoringConfig = 
            software.amazon.athena.workgroup.MonitoringConfiguration.builder()
                    .cloudWatchLoggingConfiguration(cfnCloudWatchConfig)
                    .managedLoggingConfiguration(cfnManagedConfig)
                    .s3LoggingConfiguration(cfnS3Config)
                    .build();

    Translator translator = new Translator();
    MonitoringConfiguration sdkMonitoringConfig = translator.createSdkMonitoringConfigurationFromCfnConfiguration(cfnMonitoringConfig);

    assertThat(sdkMonitoringConfig.cloudWatchLoggingConfiguration().enabled()).isTrue();
    assertThat(sdkMonitoringConfig.cloudWatchLoggingConfiguration().logGroup()).isEqualTo("/aws/athena/workgroup/test");
    assertThat(sdkMonitoringConfig.managedLoggingConfiguration().enabled()).isTrue();
    assertThat(sdkMonitoringConfig.s3LoggingConfiguration().enabled()).isTrue();
    assertThat(sdkMonitoringConfig.s3LoggingConfiguration().logLocation()).isEqualTo("s3://test-logs/");
  }

  @Test
  void testCreateSdkCloudWatchLoggingConfigurationFromCfnConfiguration() {
    // CFN format
    Map<String, List<String>> cfnLogTypes = new HashMap<>();
    cfnLogTypes.put("SPARK_DRIVER", Arrays.asList("STDOUT", "STDERR"));
    cfnLogTypes.put("SPARK_EXECUTOR", Arrays.asList("STDOUT", "STDERR"));
    
    software.amazon.athena.workgroup.CloudWatchLoggingConfiguration cfnCloudWatchConfig = 
            software.amazon.athena.workgroup.CloudWatchLoggingConfiguration.builder()
                    .enabled(true)
                    .logGroup("/aws/athena/workgroup/test")
                    .logStreamNamePrefix("session")
                    .logTypes(cfnLogTypes)
                    .build();

    Translator translator = new Translator();
    CloudWatchLoggingConfiguration sdkCloudWatchConfig = translator.createSdkCloudWatchLoggingConfigurationFromCfnConfiguration(cfnCloudWatchConfig);

    assertThat(sdkCloudWatchConfig.enabled()).isTrue();
    assertThat(sdkCloudWatchConfig.logGroup()).isEqualTo("/aws/athena/workgroup/test");
    assertThat(sdkCloudWatchConfig.logStreamNamePrefix()).isEqualTo("session");
    
    // Verify the LogTypes conversion worked correctly - using String types instead of enums
    assertThat(sdkCloudWatchConfig.logTypes()).containsKey("SPARK_DRIVER");
    assertThat(sdkCloudWatchConfig.logTypes()).containsKey("SPARK_EXECUTOR");
    assertThat(sdkCloudWatchConfig.logTypes().get("SPARK_DRIVER"))
            .containsExactly("STDOUT", "STDERR");
    assertThat(sdkCloudWatchConfig.logTypes().get("SPARK_EXECUTOR"))
            .containsExactly("STDOUT", "STDERR");
  }

  @Test
  void testCreateSdkManagedLoggingConfigurationFromCfnConfiguration() {
    software.amazon.athena.workgroup.ManagedLoggingConfiguration cfnManagedConfig = 
            software.amazon.athena.workgroup.ManagedLoggingConfiguration.builder()
                    .enabled(false)
                    .kmsKey("arn:aws:kms:us-east-1:123456789012:key/managed-key-id")
                    .build();

    Translator translator = new Translator();
    ManagedLoggingConfiguration sdkManagedConfig = translator.createSdkManagedLoggingConfigurationFromCfnConfiguration(cfnManagedConfig);

    assertThat(sdkManagedConfig.enabled()).isFalse();
    assertThat(sdkManagedConfig.kmsKey()).isEqualTo("arn:aws:kms:us-east-1:123456789012:key/managed-key-id");
  }

  @Test
  void testCreateSdkS3LoggingConfigurationFromCfnConfiguration() {
    software.amazon.athena.workgroup.S3LoggingConfiguration cfnS3Config = 
            software.amazon.athena.workgroup.S3LoggingConfiguration.builder()
                    .enabled(true)
                    .logLocation("s3://test-athena-logs/")
                    .kmsKey("arn:aws:kms:us-east-1:123456789012:key/test-key-id")
                    .build();

    Translator translator = new Translator();
    S3LoggingConfiguration sdkS3Config = translator.createSdkS3LoggingConfigurationFromCfnConfiguration(cfnS3Config);

    assertThat(sdkS3Config.enabled()).isTrue();
    assertThat(sdkS3Config.logLocation()).isEqualTo("s3://test-athena-logs/");
    assertThat(sdkS3Config.kmsKey()).isEqualTo("arn:aws:kms:us-east-1:123456789012:key/test-key-id");
  }

  @Test
  void testCreateCfnEngineConfigurationFromSdkConfiguration() {
    Map<String, String> hiveProperties = new HashMap<>();
    hiveProperties.put("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory");
    
    Classification sdkClassification = Classification.builder()
            .name("hive-site")
            .properties(hiveProperties)
            .build();
    
    EngineConfiguration sdkEngineConfig = EngineConfiguration.builder()
            .classifications(Arrays.asList(sdkClassification))
            .build();

    Translator translator = new Translator();
    software.amazon.athena.workgroup.EngineConfiguration cfnEngineConfig = translator.createCfnEngineConfigurationFromSdkConfiguration(sdkEngineConfig);

    assertThat(cfnEngineConfig.getClassifications()).hasSize(1);
    assertThat(cfnEngineConfig.getClassifications().get(0).getName()).isEqualTo("hive-site");
    assertThat(cfnEngineConfig.getClassifications().get(0).getProperties()).isEqualTo(hiveProperties);
  }

  @Test
  void testCreateCfnEngineConfigurationFromSdkConfigurationWithNullClassifications() {
    EngineConfiguration sdkEngineConfig = EngineConfiguration.builder()
            .classifications((List<Classification>) null)
            .build();

    Translator translator = new Translator();
    software.amazon.athena.workgroup.EngineConfiguration cfnEngineConfig = translator.createCfnEngineConfigurationFromSdkConfiguration(sdkEngineConfig);

    // AWS SDK v2 might return an empty list instead of null when classifications is not set
    assertThat(cfnEngineConfig.getClassifications()).isNullOrEmpty();
  }

  @Test
  void testCreateCfnMonitoringConfigurationFromSdkConfiguration() {
    CloudWatchLoggingConfiguration sdkCloudWatchConfig = CloudWatchLoggingConfiguration.builder()
            .enabled(true)
            .logGroup("/aws/athena/workgroup/prod")
            .build();
    
    ManagedLoggingConfiguration sdkManagedConfig = ManagedLoggingConfiguration.builder()
            .enabled(false)
            .build();
    
    S3LoggingConfiguration sdkS3Config = S3LoggingConfiguration.builder()
            .enabled(true)
            .logLocation("s3://prod-athena-logs/")
            .build();
    
    MonitoringConfiguration sdkMonitoringConfig = MonitoringConfiguration.builder()
            .cloudWatchLoggingConfiguration(sdkCloudWatchConfig)
            .managedLoggingConfiguration(sdkManagedConfig)
            .s3LoggingConfiguration(sdkS3Config)
            .build();

    Translator translator = new Translator();
    software.amazon.athena.workgroup.MonitoringConfiguration cfnMonitoringConfig = translator.createCfnMonitoringConfigurationFromSdkConfiguration(sdkMonitoringConfig);

    assertThat(cfnMonitoringConfig.getCloudWatchLoggingConfiguration().getEnabled()).isTrue();
    assertThat(cfnMonitoringConfig.getCloudWatchLoggingConfiguration().getLogGroup()).isEqualTo("/aws/athena/workgroup/prod");
    assertThat(cfnMonitoringConfig.getManagedLoggingConfiguration().getEnabled()).isFalse();
    assertThat(cfnMonitoringConfig.getS3LoggingConfiguration().getEnabled()).isTrue();
    assertThat(cfnMonitoringConfig.getS3LoggingConfiguration().getLogLocation()).isEqualTo("s3://prod-athena-logs/");
  }

  @Test
  void testCreateCfnCloudWatchLoggingConfigurationFromSdkConfiguration() {
    // Create SDK logTypes using String types (AWS SDK v2 uses Map<String, List<String>> instead of enums)
    Map<String, List<String>> sdkLogTypes = new HashMap<>();
    sdkLogTypes.put("SPARK_EXECUTOR", Arrays.asList("STDOUT"));
    
    // Expected CFN logTypes (what we expect to get back)
    Map<String, List<String>> expectedCfnLogTypes = new HashMap<>();
    expectedCfnLogTypes.put("SPARK_EXECUTOR", Arrays.asList("STDOUT"));
    
    CloudWatchLoggingConfiguration sdkCloudWatchConfig = CloudWatchLoggingConfiguration.builder()
            .enabled(false)
            .logGroup("/aws/athena/workgroup/dev")
            .logStreamNamePrefix("dev-session")
            .logTypes(sdkLogTypes)
            .build();

    Translator translator = new Translator();
    software.amazon.athena.workgroup.CloudWatchLoggingConfiguration cfnCloudWatchConfig = translator.createCfnCloudWatchLoggingConfigurationFromSdkConfiguration(sdkCloudWatchConfig);

    assertThat(cfnCloudWatchConfig.getEnabled()).isFalse();
    assertThat(cfnCloudWatchConfig.getLogGroup()).isEqualTo("/aws/athena/workgroup/dev");
    assertThat(cfnCloudWatchConfig.getLogStreamNamePrefix()).isEqualTo("dev-session");
    assertThat(cfnCloudWatchConfig.getLogTypes()).isEqualTo(expectedCfnLogTypes);
  }

  @Test
  void testCreateCfnManagedLoggingConfigurationFromSdkConfiguration() {
    ManagedLoggingConfiguration sdkManagedConfig = ManagedLoggingConfiguration.builder()
            .enabled(true)
            .kmsKey("arn:aws:kms:us-west-2:123456789012:key/managed-logging-key")
            .build();

    Translator translator = new Translator();
    software.amazon.athena.workgroup.ManagedLoggingConfiguration cfnManagedConfig = translator.createCfnManagedLoggingConfigurationFromSdkConfiguration(sdkManagedConfig);

    assertThat(cfnManagedConfig.getEnabled()).isTrue();
    assertThat(cfnManagedConfig.getKmsKey()).isEqualTo("arn:aws:kms:us-west-2:123456789012:key/managed-logging-key");
  }

  @Test
  void testCreateCfnS3LoggingConfigurationFromSdkConfiguration() {
    S3LoggingConfiguration sdkS3Config = S3LoggingConfiguration.builder()
            .enabled(false)
            .logLocation("s3://dev-athena-logs/queries/")
            .kmsKey("arn:aws:kms:us-west-2:123456789012:key/dev-key-id")
            .build();

    Translator translator = new Translator();
    software.amazon.athena.workgroup.S3LoggingConfiguration cfnS3Config = translator.createCfnS3LoggingConfigurationFromSdkConfiguration(sdkS3Config);

    assertThat(cfnS3Config.getEnabled()).isFalse();
    assertThat(cfnS3Config.getLogLocation()).isEqualTo("s3://dev-athena-logs/queries/");
    assertThat(cfnS3Config.getKmsKey()).isEqualTo("arn:aws:kms:us-west-2:123456789012:key/dev-key-id");
  }

  @Test
  void testCreateSdkMonitoringConfigurationFromCfnConfigurationWithNullComponents() {
    software.amazon.athena.workgroup.MonitoringConfiguration cfnMonitoringConfig = 
            software.amazon.athena.workgroup.MonitoringConfiguration.builder()
                    .cloudWatchLoggingConfiguration(null)
                    .managedLoggingConfiguration(null)
                    .s3LoggingConfiguration(null)
                    .build();

    Translator translator = new Translator();
    MonitoringConfiguration sdkMonitoringConfig = translator.createSdkMonitoringConfigurationFromCfnConfiguration(cfnMonitoringConfig);

    assertThat(sdkMonitoringConfig.cloudWatchLoggingConfiguration()).isNull();
    assertThat(sdkMonitoringConfig.managedLoggingConfiguration()).isNull();
    assertThat(sdkMonitoringConfig.s3LoggingConfiguration()).isNull();
  }

  @Test
  void testCreateCfnMonitoringConfigurationFromSdkConfigurationWithNullComponents() {
    MonitoringConfiguration sdkMonitoringConfig = MonitoringConfiguration.builder()
            .cloudWatchLoggingConfiguration((CloudWatchLoggingConfiguration) null)
            .managedLoggingConfiguration((ManagedLoggingConfiguration) null)
            .s3LoggingConfiguration((S3LoggingConfiguration) null)
            .build();

    Translator translator = new Translator();
    software.amazon.athena.workgroup.MonitoringConfiguration cfnMonitoringConfig = translator.createCfnMonitoringConfigurationFromSdkConfiguration(sdkMonitoringConfig);

    assertThat(cfnMonitoringConfig.getCloudWatchLoggingConfiguration()).isNull();
    assertThat(cfnMonitoringConfig.getManagedLoggingConfiguration()).isNull();
    assertThat(cfnMonitoringConfig.getS3LoggingConfiguration()).isNull();
  }

  private software.amazon.awssdk.services.athena.model.WorkGroupConfiguration.Builder getBareMinSdkWorkGroupConfigBuilder() {
    return WorkGroupConfiguration.builder()
            .enforceWorkGroupConfiguration(true)
            .publishCloudWatchMetricsEnabled(true)
            .requesterPaysEnabled(false)
            .resultConfiguration(ResultConfiguration.builder()
                    .outputLocation("s3://abc/")
                    .encryptionConfiguration(EncryptionConfiguration.builder()
                            .encryptionOption("CSE_KMS")
                            .kmsKey("some_key")
                            .build())
                    .expectedBucketOwner("123456789012")
                    .aclConfiguration(AclConfiguration.builder().s3AclOption("BUCKET_OWNER_FULL_CONTROL").build())
                    .build())
            .additionalConfiguration("{\"additionalConfig\": \"some_config\"}");
  }

  private software.amazon.athena.workgroup.WorkGroupConfiguration.WorkGroupConfigurationBuilder getBareMinCfnWorkGroupConfiguration(){
    return software.amazon.athena.workgroup.WorkGroupConfiguration.builder()
            .enforceWorkGroupConfiguration(true)
            .publishCloudWatchMetricsEnabled(true)
            .requesterPaysEnabled(false)
            .resultConfiguration(software.amazon.athena.workgroup.ResultConfiguration.builder()
                    .outputLocation("s3://test/")
                    .encryptionConfiguration(software.amazon.athena.workgroup.EncryptionConfiguration.builder()
                            .encryptionOption("CSE_KMS")
                            .kmsKey("some_key")
                            .build())
                    .expectedBucketOwner("123456789012")
                    .aclConfiguration(software.amazon.athena.workgroup.AclConfiguration.builder().s3AclOption("BUCKET_OWNER_FULL_CONTROL").build())
                    .build())
            .additionalConfiguration("{\"additionalConfig\": \"some_config\"}");
  }
}
