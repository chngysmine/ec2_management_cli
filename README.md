# EC2 Management CLI & Web Interface

Công cụ quản lý EC2 instances và EBS volumes trên AWS với giao diện CLI và Web đầy đủ tính năng.

## Tính năng

### CLI Interface
- ✅ Quản lý EC2 Instances: list, create, start, stop, terminate
- ✅ Quản lý EBS Volumes: list, attach, detach, set delete-on-termination
- ✅ Báo cáo Inventory: xuất JSON hoặc CSV
- ✅ Phân tích Cost Optimization: tìm idle instances và orphaned volumes
- ✅ Filtering: theo tags, states, regions
- ✅ Multi-region support

### Web Interface
- ✅ Dashboard hiện đại với terminal-style UI
- ✅ Quản lý Instances qua giao diện web
- ✅ Quản lý Volumes
- ✅ Báo cáo Inventory và Cost Optimization
- ✅ JSON API endpoints cho terminal UI
- ✅ Basic Authentication (tùy chọn)

## Yêu cầu

- Python >= 3.11
- AWS Credentials được cấu hình (AWS CLI, IAM roles, hoặc SSO)
- Quyền truy cập AWS EC2 và CloudWatch

## Cài đặt

### Cài đặt từ source

```bash
# Clone repository
git clone <repository-url>
cd ec2_management_cli

# Cài đặt dependencies
pip install -r requirements.txt

# Cài đặt package
pip install -e .
```

### Cấu hình AWS Credentials

### Option 1: Dùng Mock Mode (Không cần AWS Account - Khuyến nghị cho testing)

**KHÔNG CẦN** AWS account hay thẻ tín dụng! Chạy với mock data:

```bash
# Chạy web server với mock data
python run_mock_mode.py

# Mở trình duyệt
# http://localhost:8000?region=us-east-1
```

Mock mode bao gồm:
- 3 test instances (running/stopped states)
- 2 test volumes (available/in-use)
- Cost optimization report với sample data

### Option 2: Dùng AWS thực tế

Có nhiều cách để cấu hình AWS credentials:

1. **AWS CLI** (khuyến nghị):
```bash
aws configure
```

2. **Environment Variables**:
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
```

3. **AWS Profile**:
```bash
export AWS_PROFILE=your_profile_name
```

4. **IAM Roles** (khi chạy trên EC2)

## Sử dụng

### CLI Commands

#### Quản lý Instances

```bash
# Liệt kê tất cả instances
ec2-man instance list

# Liệt kê instances với filter
ec2-man instance list --tag Environment=Production --state running

# Tạo instance mới từ file config YAML
ec2-man instance create examples/create_dev_instance.yaml

# Start instance
ec2-man instance start i-1234567890abcdef0

# Stop instance
ec2-man instance stop i-1234567890abcdef0

# Terminate instance
ec2-man instance terminate i-1234567890abcdef0

# Chỉ định region
ec2-man --region us-west-2 instance list
```

#### Quản lý Volumes

```bash
# Liệt kê volumes
ec2-man volume list

# Liệt kê volumes available
ec2-man volume list --status available

# Attach volume
ec2-man volume attach vol-1234567890abcdef0 i-1234567890abcdef0 /dev/sdf

# Detach volume
ec2-man volume detach vol-1234567890abcdef0

# Force detach
ec2-man volume detach vol-1234567890abcdef0 --force

# Set delete-on-termination
ec2-man volume set-delete-on-term i-1234567890abcdef0 /dev/sda1 --enable
```

#### Báo cáo

```bash
# Inventory report (JSON)
ec2-man report inventory

# Inventory report (CSV)
ec2-man report inventory --output csv > inventory.csv

# Inventory cho specific regions
ec2-man report inventory --regions us-east-1 us-west-2

# Cost optimization report
ec2-man report cost-optimize

# Cost optimization với custom threshold
ec2-man report cost-optimize --idle-threshold 10.0

# Cost optimization cho specific regions
ec2-man report cost-optimize --regions us-east-1
```

### Web Interface

#### Khởi động Web Server

```bash
# Sử dụng entry point
ec2-man-web

# Hoặc chạy trực tiếp
python -m ec2_manager.web_flask

# Chỉ định port
PORT=8080 ec2-man-web
```

#### Truy cập Web Interface

Mở trình duyệt và truy cập: `http://localhost:8000`

**Lưu ý**: Bạn cần chỉ định region trong URL hoặc set environment variable:
- `http://localhost:8000?region=us-east-1`
- Hoặc set `AWS_REGION` hoặc `AWS_DEFAULT_REGION`

#### Basic Authentication (Tùy chọn)

Để bật authentication, set environment variables:

```bash
export EC2_MAN_WEB_USER=admin
export EC2_MAN_WEB_PASS=your_password
ec2-man-web
```

#### Web Interface Features

1. **Dashboard** (`/`): Terminal-style interface với stats và controls
2. **Instances** (`/instances`): Xem và quản lý instances
3. **Volumes** (`/volumes`): Xem volumes
4. **Inventory Report** (`/reports/inventory`): Báo cáo inventory
5. **Cost Optimization** (`/reports/cost-optimize`): Phân tích chi phí

#### API Endpoints

Web interface cũng cung cấp JSON API:

```bash
# List instances
curl "http://localhost:8000/api/instances?region=us-east-1"

# Start instance
curl -X POST "http://localhost:8000/api/instances/start?region=us-east-1" \
  -H "Content-Type: application/json" \
  -d '{"instance_id": "i-1234567890abcdef0"}'

# Stop instance
curl -X POST "http://localhost:8000/api/instances/stop?region=us-east-1" \
  -H "Content-Type: application/json" \
  -d '{"instance_id": "i-1234567890abcdef0"}'

# Terminate instance
curl -X POST "http://localhost:8000/api/instances/terminate?region=us-east-1" \
  -H "Content-Type: application/json" \
  -d '{"instance_id": "i-1234567890abcdef0"}'

# Inventory report
curl "http://localhost:8000/api/reports/inventory?regions=us-east-1,us-west-2"

# Cost optimization report
curl "http://localhost:8000/api/reports/cost-optimize?threshold=5.0&regions=us-east-1"
```

## Cấu hình Instance Creation

File YAML config cho instance creation:

```yaml
instance:
  ImageId: "ami-0abcdef1234567890"
  InstanceType: "t4g.medium"
  KeyName: "dev-key-us-east-1"
  MinCount: 1
  MaxCount: 1

network:
  SubnetId: "subnet-0123456789abcdef"
  SecurityGroupIds:
    - "sg-0abcdef1234567890"

user_data: |
  #!/bin/bash
  yum update -y
  yum install -y nginx
  systemctl start nginx
  systemctl enable nginx

tags:
  - Key: "Name"
    Value: "dev-web-server"
  - Key: "Environment"
    Value: "Development"
  - Key: "CostCenter"
    Value: "CC-DEV-001"
```

## Cấu trúc Dự án

```
ec2_management_cli/
├── ec2_manager/
│   ├── __init__.py          # Package initialization
│   ├── cli.py               # CLI interface (Click)
│   ├── core.py              # Core EC2Manager class
│   ├── web_flask.py         # Flask web interface
│   ├── web.py               # FastAPI web interface (alternative)
│   ├── utils.py             # Utility functions
│   ├── exceptions.py        # Custom exceptions
│   └── templates/           # HTML templates
│       ├── home.html
│       ├── instances.html
│       ├── volumes.html
│       ├── inventory.html
│       ├── cost_optimize.html
│       └── error.html
├── examples/
│   └── create_dev_instance.yaml
├── requirements.txt
├── setup.py
└── README.md
```

## IAM Permissions

Để sử dụng đầy đủ tính năng, IAM user/role cần các permissions sau:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeInstances",
        "ec2:DescribeVolumes",
        "ec2:DescribeRegions",
        "ec2:RunInstances",
        "ec2:StartInstances",
        "ec2:StopInstances",
        "ec2:TerminateInstances",
        "ec2:AttachVolume",
        "ec2:DetachVolume",
        "ec2:ModifyInstanceAttribute",
        "cloudwatch:GetMetricStatistics",
        "sts:GetCallerIdentity"
      ],
      "Resource": "*"
    }
  ]
}
```

## Troubleshooting

### Lỗi "AWS credentials not found"

- Kiểm tra AWS credentials đã được cấu hình: `aws sts get-caller-identity`
- Đảm bảo environment variables hoặc AWS profile đã được set

### Lỗi "You must specify a region"

- Set `AWS_REGION` hoặc `AWS_DEFAULT_REGION` environment variable
- Hoặc thêm `?region=us-east-1` vào URL khi dùng web interface
- Hoặc dùng `--region` flag trong CLI

### Web interface không kết nối được

- Kiểm tra port đã được mở: `netstat -an | grep 8000`
- Kiểm tra firewall settings
- Đảm bảo region đã được chỉ định

### Cost optimization không hiển thị data

- CloudWatch metrics có thể mất vài phút để có data
- Đảm bảo instances đã chạy ít nhất vài giờ
- Kiểm tra IAM permissions cho CloudWatch

## Development

### Setup Development Environment

```bash
# Clone và install
git clone <repository-url>
cd ec2_management_cli
pip install -r requirements.txt
pip install -e .

# Run tests (nếu có)
pytest
```

### Verbose Logging

```bash
# CLI với verbose logging
ec2-man -vv instance list

# Web với logging
# Logs sẽ được output dưới dạng JSON
```

## License

[Specify your license here]

## Contributing

[Contributing guidelines if applicable]
