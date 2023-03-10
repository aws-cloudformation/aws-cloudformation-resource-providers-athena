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
    assertThat(cfnWorkGroupConfiguration.getBytesScannedCutoffPerQuery()).isEqualTo(sdkWorkGroupConfiguration.bytesScannedCutoffPerQuery());
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
