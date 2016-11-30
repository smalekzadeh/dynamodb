# dynamodb

This is a dynamodb play project created to investigate the available options in table read and write.


# Install

Set Up the AWS Command Line Interface (AWS CLI).
http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-set-up.html


Create a venv environment and activate it.


Then install the dependencies.



## dynamodb table
The table created for this exercise is called "callstab":

Table name	callstab
Primary partition key	callid (String)
Primary sort key	-
Item count	1,000,000




| Index name               | partition key      | sort key          |
|--------------------------|--------------------|-------------------|
| trunkid-calldate-index   | trunkid (String)   | calldate (Number) |
| accountid-calldate-index | accountid (String) | calldate (Number) |
| location-calldate-index  | location (String)  | calldate (Number) |



## usage

Run the query.py and select a query from the available options.
The queries are basic index queries.

You could run similar queries using AWS dynamodb console.