/*
    Resource: Lambda layers
    Description: Contain the following libs
      - cx_Oracle
    Author: Edson. G. A. Correia
*/
resource "aws_lambda_layer_version" "oracle_connector" {
  s3_bucket = aws_s3_bucket.rsm-artifacts.id
  s3_key = aws_s3_object.zip_oracle_lib.key
  layer_name = "oracle_connector"
  description = "Connect to Oracle databases."
  compatible_runtimes = ["python3.7"]
  compatible_architectures = ["x86_64"]
  depends_on = [aws_s3_bucket.rsm-artifacts, aws_s3_object.zip_oracle_lib]
}
#
resource "aws_s3_object" "zip_oracle_lib" {
  bucket = aws_s3_bucket.rsm-artifacts.id
  key    = "cx_oracle.zip"
  source = "layers/"
}

/*
    Resource: Lambda layers
    Name: fastparquet
    Description: Contain the following libs
      - fastparquet
    Author: Edson G. A. Correia

resource "aws_lambda_layer_version" "fastparquet" {
  filename   = "custom_layers/fastparquet/fastparquet.zip"
  layer_name = "fastparquet"
  compatible_runtimes = ["python3.8"]
}
*/
