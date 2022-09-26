/*
    Resource: Lambda function
    Name: oracle_extraction
    Description: Do the ingestion process
    Author: Edson G. A. Correia
*/
resource "aws_lambda_function" "oracle_extraction" {
  filename = "lambda_src/extraction/function.zip"
  function_name = "oracle_extraction"
  description = "Performs the extraction from oracle DB"
  role = aws_iam_role.datalake-role.arn
  handler = "lambda.lambda_handler"
  layers = [aws_lambda_layer_version.oracle_connector.arn]
  ephemeral_storage {
    size = 10240
  }
  memory_size = 10240
  runtime = "python3.7"
  timeout = 900
  reserved_concurrent_executions = -1
  depends_on = [aws_lambda_layer_version.oracle_connector]
  tags = {
    project = "data-lake"
  }
}

/*
    Resource: Lambda function
    Name: transformation
    Description:
    Author: Edson G. A. Correia
*/
resource "aws_lambda_function" "transformation" {
  filename = "lambda_src/transform/function.zip"
  function_name = "data_transformation"
  description = "Function dedicated to clean, transform and validate the data"
  role = aws_iam_role.datalake-role.arn
  handler = "lambda.lambda_handler"
  layers = ["arn:aws:lambda:us-east-1:336392948345:layer:AWSDataWrangler-Python39:9"]
  ephemeral_storage {
    size = 10240
  }
  memory_size = 10240
  runtime = "python3.9"
  timeout = 900
  reserved_concurrent_executions = -1
  tags = {
    project = "data-lake"
  }
}

/*
    Resource: Lambda function
    Name: redshift_loader
    Description:
    Author: Edson G. A. Correia
*/
resource "aws_lambda_function" "redshift_loader" {
  filename = "lambda_src/load/function.zip"
  function_name = "redshift_loader"
  description = "Function to load data into Redshift"
  role = aws_iam_role.datalake-role.arn
  handler = "lambda.lambda_handler"
  layers = [
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSDataWrangler-Python39:9",
    "arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p38-aws-psycopg2:1"
  ]
  ephemeral_storage {
    size = 10240
  }
  memory_size = 10240
  runtime = "python3.9"
  timeout = 900
  reserved_concurrent_executions = -1
  tags = {
    project = "data-lake"
  }
}
