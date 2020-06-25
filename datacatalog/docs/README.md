# AWS::Athena::DataCatalog

Resource schema for AWS::Athena::DataCatalog

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::Athena::DataCatalog",
    "Properties" : {
        "<a href="#name" title="Name">Name</a>" : <i>String</i>,
        "<a href="#description" title="Description">Description</a>" : <i>String</i>,
        "<a href="#parameters" title="Parameters">Parameters</a>" : <i><a href="parameters.md">Parameters</a></i>,
        "<a href="#tags" title="Tags">Tags</a>" : <i>[ <a href="tags.md">Tags</a>, ... ]</i>,
        "<a href="#type" title="Type">Type</a>" : <i>String</i>
    }
}
</pre>

### YAML

<pre>
Type: AWS::Athena::DataCatalog
Properties:
    <a href="#name" title="Name">Name</a>: <i>String</i>
    <a href="#description" title="Description">Description</a>: <i>String</i>
    <a href="#parameters" title="Parameters">Parameters</a>: <i><a href="parameters.md">Parameters</a></i>
    <a href="#tags" title="Tags">Tags</a>: <i>
      - <a href="tags.md">Tags</a></i>
    <a href="#type" title="Type">Type</a>: <i>String</i>
</pre>

## Properties

#### Name

The name of the data catalog to create. The catalog name must be unique for the AWS account and can use a maximum of 128 alphanumeric, underscore, at sign, or hyphen characters. 

_Required_: Yes

_Type_: String

_Minimum_: <code>1</code>

_Maximum_: <code>256</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### Description

A description of the data catalog to be created. 

_Required_: No

_Type_: String

_Minimum_: <code>1</code>

_Maximum_: <code>1024</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Parameters

Specifies the Lambda function or functions to use for creating the data catalog. This is a mapping whose values depend on the catalog type. 

_Required_: No

_Type_: <a href="parameters.md">Parameters</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Tags

_Required_: No

_Type_: List of <a href="tags.md">Tags</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Type

The type of data catalog to create: LAMBDA for a federated catalog, GLUE for AWS Glue Catalog, or HIVE for an external hive metastore. 

_Required_: Yes

_Type_: String

_Allowed Values_: <code>LAMBDA</code> | <code>GLUE</code> | <code>HIVE</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

## Return Values

### Ref

When you pass the logical ID of this resource to the intrinsic `Ref` function, Ref returns the Name.
