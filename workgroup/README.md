# AWS::Athena::WorkGroup

Congratulations on starting development! Next steps:

1. Write the JSON schema describing your resource, `aws-athena-workgroup.json`
2. The RPDK will automatically generate the correct resource model from the
   schema whenever the project is built via Maven. You can also do this manually
   with the following command: `cfn-cli generate`
3. Configuration files - .rpdk-config, .gitignore, lombok.config, pom.xml
4. Implement your Create, Update, Read, List and Delete resource handlers for the workgroup resource.
5. PolicyFile for CloudFormation : resource-role.yaml. It is required to determine the assume role required to create the resource

Note : The file template.yml is used for running tests via AWS SAM.

Please don't modify files under `target/generated-sources/rpdk`, as they will be
automatically overwritten.

The code use [Lombok](https://projectlombok.org/), and [you may have to install
IDE integrations](https://projectlombok.org/) to enable auto-complete for
Lombok-annotated classes
