# AWS::Athena::PreparedStatement

Resource schema for AWS::Athena::PreparedStatement

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::Athena::PreparedStatement",
    "Properties" : {
        "<a href="#statementname" title="StatementName">StatementName</a>" : <i>String</i>,
        "<a href="#workgroup" title="WorkGroup">WorkGroup</a>" : <i>String</i>,
        "<a href="#description" title="Description">Description</a>" : <i>String</i>,
        "<a href="#querystatement" title="QueryStatement">QueryStatement</a>" : <i>String</i>
    }
}
</pre>

### YAML

<pre>
Type: AWS::Athena::PreparedStatement
Properties:
    <a href="#statementname" title="StatementName">StatementName</a>: <i>String</i>
    <a href="#workgroup" title="WorkGroup">WorkGroup</a>: <i>String</i>
    <a href="#description" title="Description">Description</a>: <i>String</i>
    <a href="#querystatement" title="QueryStatement">QueryStatement</a>: <i>String</i>
</pre>

## Properties

#### StatementName

The name of the prepared statement.

_Required_: Yes

_Type_: String

_Minimum Length_: <code>1</code>

_Maximum Length_: <code>256</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### WorkGroup

The name of the workgroup to which the prepared statement belongs.

_Required_: Yes

_Type_: String

_Minimum Length_: <code>1</code>

_Maximum Length_: <code>128</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### Description

The description of the prepared statement.

_Required_: No

_Type_: String

_Minimum Length_: <code>1</code>

_Maximum Length_: <code>1024</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### QueryStatement

The query string for the prepared statement.

_Required_: Yes

_Type_: String

_Minimum Length_: <code>1</code>

_Maximum Length_: <code>262144</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)
