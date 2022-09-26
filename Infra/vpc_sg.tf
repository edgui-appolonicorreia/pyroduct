/*
  Resource: VPC
  Name: Data-Lake VPC
  Author: Edson G. A. Correia
*/

resource "aws_vpc" "datalake-vpc" {
  cidr_block = "192.0.0.0/24"
  enable_dns_hostnames = true
  tags = {
    Name = "datalake-vpc"
    environment = "prod"
    project = "data-lake"
  }
}

resource "aws_subnet" "datalake-subnet-uea" {
  vpc_id = aws_vpc.datalake-vpc.id
  cidr_block = "10.0.1.0/24"
  availability_zone = "us-east-1a"
  depends_on = [aws_vpc.datalake-vpc]
  tags = {
    Name = "datalake-subnet-uea"
    environment = "prod"
    project = "data-lake"
  }
}

resource "aws_subnet" "datalake-subnet-ueb" {
  vpc_id = aws_vpc.datalake-vpc.id
  cidr_block = "10.0.2.0/24"
  availability_zone = "us-east-1b"
  depends_on = [aws_vpc.datalake-vpc]
  tags = {
    Name = "datalake-subnet-ueb"
    environment = "prod"
    project = "data-lake"
  }
}

resource "aws_subnet" "datalake-subnet-uec" {
  vpc_id = aws_vpc.datalake-vpc.id
  cidr_block = "10.0.3.0/24"
  availability_zone = "us-east-1c"
  depends_on = [aws_vpc.datalake-vpc]
  tags = {
    Name = "datalake-subnet-uec"
    environment = "prod"
    project = "data-lake"
  }
}

resource "aws_route_table" "datalake-route-table" {
  vpc_id = aws_vpc.datalake-vpc.id
  depends_on = [aws_vpc.datalake-vpc, aws_internet_gateway.datalake-gw]
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.datalake-gw.id
  }
  tags = {
    Name = "datalake-route-table"
    environment = "prod"
    project = "data-lake"
  }
}

resource "aws_main_route_table_association" "datalake-rt-association" {
  route_table_id = aws_route_table.datalake-route-table.id
  vpc_id = aws_vpc.datalake-vpc.id
  depends_on = [aws_internet_gateway.datalake-gw, aws_route_table.datalake-route-table]
}

/*
  Resource: Gateway
  Name: DataLake-gw
  Author: Edson G. A. Correia
*/
resource "aws_internet_gateway" "datalake-gw" {
  vpc_id = aws_vpc.datalake-vpc.id
  tags = {
    Name = "datalake"
    environment = "prod"
    project = "data-lake"
  }
}

/*
  Resource: Subnet group
  Name: DataLake-subnet-group
  Author: Edson G. A. Correia
*/
resource "aws_db_subnet_group" "datalake-subnet-group" {
  name = "datalake-subnet-group"
  subnet_ids = [
    aws_subnet.datalake-subnet-uea.id,
    aws_subnet.datalake-subnet-ueb.id,
    aws_subnet.datalake-subnet-uec
  ]
  tags = {
    environment = "prod"
    project = "data-lake"
  }
}

/*
  Resource: Security Group
  Name: DataLake Security Group
  Author: Edson G. A. Correia
*/
resource "aws_security_group" "datalake-sg" {
  name = "datalake-sg"
  description = "Allow inbound traffic"
  vpc_id = aws_vpc.datalake-vpc.id

  ingress {
    description = "Allow all access, from anywhere."
    from_port = 0
    to_port = 0
    protocol = "all"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port = 0
    to_port = 0
    protocol = "all"
    cidr_blocks = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    project = "data-lake"
  }
}
