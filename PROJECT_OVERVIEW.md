## Tổng quan dự án

**Dự án `ec2_management_cli` là bộ công cụ quản lý AWS EC2 & EBS** gồm:
- **CLI** dòng lệnh để quản lý EC2 Instances, EBS Volumes và sinh các báo cáo.
- **Web UI kiểu “terminal dashboard” (Flask/FastAPI cũ)** dùng chung core Python.
- **Kiến trúc hiện đại mới**: backend FastAPI async + frontend React/TypeScript để xây dựng dashboard hiện đại hơn, có mock mode không cần AWS.

Dự án phục vụ cả **môi trường thật trên AWS** lẫn **môi trường demo/mock** cho mục đích học tập, thử nghiệm mà không cần tài khoản AWS.

---

## Ngôn ngữ & công nghệ chính

- **Backend / CLI**:  
  - **Python 3.11+**  
  - Thư viện chính: `boto3`, `botocore`, `click`, `Flask`, `Jinja2`, `fastapi`, `uvicorn`, `pydantic`, `anyio`, `httpx`.  
- **Backend hiện đại (`modern_backend`)**:  
  - **FastAPI (async)**, kiến trúc dạng **ports/adapters (Clean-ish Architecture)** với:
    - `domain`: các model thuần Pydantic (`Instance`, `Volume`, `EventEntry`, `DashboardStats`, `OverviewPayload`, …).
    - `ports`: interface `IEC2Repository` / `AbstractEC2Repository` làm abstraction cho hạ tầng EC2.
    - `adapters`: `AwsEC2Adapter` (nói chuyện với AWS thật, bọc `EC2Manager`) và `MockEC2Adapter` (mock in‑memory có state machine & latency giả lập).
    - `api`: các `APIRouter` FastAPI cho instances, volumes, events, overview.
- **Frontend hiện đại (`modern_frontend`)**:
  - **React 18 + TypeScript + Vite**.
  - **TailwindCSS**, `@tanstack/react-query`, `axios`, `recharts`, các component UI (button, card, table, badge).
  - Kết nối tới backend mới (`modern_backend`) qua API `/api/v1/...`.

---

## Kiến trúc tổng thể & “bộ máy chính”

### 1. Core cũ: `ec2_manager` (Python package)

- **Lớp trung tâm: `EC2Manager`** (`ec2_manager/core.py`):
  - Quản lý kết nối tới AWS:
    - Dùng `boto3_session.Session` với cấu hình retry (`DEFAULT_BOTO_CONFIG`).
    - Hỗ trợ **AWS profile** (`AWS_PROFILE`, `AWS_DEFAULT_PROFILE`), **region** và **LocalStack** thông qua `LOCALSTACK_ENDPOINT` / `AWS_ENDPOINT_URL`.
    - Tự set credential dummy `"test"` khi dùng LocalStack để không cần AWS thật.
  - Cung cấp các **hàm nghiệp vụ chính**:
    - **Instances**:
      - `list_instances(tags_filter, states)` – đọc EC2 bằng `describe_instances`, trả về list dict gọn (AccountId, InstanceId, InstanceType, State, AZ, IP, NameTag, LaunchTime, …).
      - `create_instance(config)` – tạo instance từ cấu hình YAML (`instance`, `network`, `user_data`, `tags`), sinh `ClientToken` idempotent, chờ đến khi instance `running`.
      - `start_instance(instance_id)`, `stop_instance(instance_id)`, `terminate_instance(instance_id)` – kiểm tra state hiện tại rồi gọi `start_instances`, `stop_instances`, `terminate_instances` + waiter chờ hoàn tất.
    - **Volumes**:
      - `list_volumes(status_filter)` – dùng `describe_volumes`, gom thông tin `VolumeId`, `SizeGiB`, `State`, `Type`, `Iops`, `Throughput`, `AZ`, `AttachedInstances`.
      - `attach_volume(volume_id, instance_id, device_name)` / `detach_volume(volume_id, force)` – wrap API EC2.
      - `set_delete_on_termination(instance_id, device_name, delete_on_term)` – chỉnh `ModifyInstanceAttribute` cho block device mapping.
    - **Reports**:
      - `_all_regions()` – lấy danh sách region hoạt động.
      - `generate_inventory_report(regions)` – lặp all regions, gọi `describe_instances` từng region, gom thành inventory (multi‑region).
      - `_average_cpu_utilization(instance_id, region, days)` – đọc CloudWatch metric `CPUUtilization`.
      - `find_wasteful_resources(regions, idle_cpu_threshold)` – tìm:
        - **Orphaned volumes**: volumes `status=available`.
        - **Idle instances**: instances `running` có **CPU trung bình 14 ngày < threshold**.

- **CLI: `ec2_manager/cli.py`**:
  - Dùng **Click** để định nghĩa command `ec2-man` (entry point được khai báo trong `setup.py / ec2_manager.egg-info/entry_points.txt`).
  - `@click.group` `main_cli`:
    - Option chung: `--region`, `-v/--verbose` (thiết lập logger).
  - Nhóm lệnh **`instance`**:
    - `ec2-man instance list --tag Key=Value --state running` → gọi `EC2Manager.list_instances`, in JSON.
    - `ec2-man instance create examples/create_dev_instance.yaml` → load YAML (`load_config`) rồi gọi `create_instance`.
    - `ec2-man instance start/stop/terminate INSTANCE_ID`.
  - Nhóm lệnh **`volume`**:
    - `volume list --status available/running`.
    - `volume attach VOLUME_ID INSTANCE_ID /dev/sdf`.
    - `volume detach VOLUME_ID --force`.
    - `volume set-delete-on-term INSTANCE_ID DEVICE --enable/--disable`.
  - Nhóm lệnh **`report`**:
    - `report inventory --output json|csv --regions us-east-1 ...`.
    - `report cost-optimize --regions ... --idle-threshold 5.0`.
  - Tất cả lệnh đều:
    - Khởi tạo `EC2Manager(region)`.
    - Bắt `AWSAuthError`, `OperationError`, return error code 1 nếu lỗi.

- **Web cũ**:
  - **Flask Web + JSON API** – file `ec2_manager/web_flask.py`:
    - Khởi tạo `Flask` + `setup_logging`.
    - **Basic Auth** đơn giản với `EC2_MAN_WEB_USER` / `EC2_MAN_WEB_PASS`.
    - `get_manager()` đọc `region` từ query hoặc env; có thể override `AWS_PROFILE` qua `?profile=...`.
    - View HTML:
      - `/` – trang home terminal‑style.
      - `/instances` – bảng instances, filter `state`, cho phép start/stop/terminate qua form POST.
      - `/volumes` – danh sách volumes + dropdown instance chạy để attach/detach, set delete‑on‑termination.
      - `/reports/inventory`, `/reports/cost-optimize` – render template với inventory + báo cáo tối ưu chi phí.
    - JSON API cho terminal UI:
      - `/api/instances` (GET) – trả list instances.
      - `/api/instances/start|stop|terminate` (POST) – điều khiển lifecycle instance.
      - `/api/reports/inventory`, `/api/reports/cost-optimize` (GET) – trả báo cáo JSON.
    - Error handler cho `AWSAuthError`, `OperationError`, exception chung → trả HTML hoặc JSON tuỳ `Accept` header.
    - `run()` dùng `DEFAULT_WEB_PORT` (cấu hình trong `ec2_manager/__init__.py`) để `app.run(...)`.
  - **FastAPI Web đơn giản** – file `ec2_manager/web.py`:
    - FastAPI + Jinja2 template tương tự Flask, nhưng ít endpoint hơn (chủ yếu HTML, không có JSON REST như Flask).
    - Có hàm `run()` chạy bằng `uvicorn`.

### 2. Backend hiện đại: `modern_backend`

- **Điểm vào chính**: `modern_backend/main.py`
  - Khởi tạo `FastAPI` với title `"Modern EC2 Manager"`, version, description tiếng Việt.
  - Đăng ký router:
    - `routes_instances` – `/api/v1/instances`, `/api/v1/economics`, `/api/v1/instances/throttled`.
    - `routes_volumes` – `/api/v1/volumes`.
    - `routes_events` – (xem file tương ứng, cung cấp event log).
    - `routes_overview` – `/api/v1/overview`.
  - Healthcheck: `GET /healthz` trả `{ "status": "ok" }`.
  - Có block `if __name__ == "__main__":` để chạy `uvicorn modern_backend.main:app --port 8001 --reload`.

- **Dependency injection & chế độ hoạt động (`modern_backend/api/dependencies.py`)**:
  - Hàm `_get_repo()` được cache bằng `@lru_cache`, tạo **một instance `IEC2Repository` duy nhất**:
    - Đọc `APP_MODE` (mặc định `"MOCK"`).
    - Đọc region từ `AWS_REGION` / `AWS_DEFAULT_REGION` hoặc `"us-east-1"`.
    - Nếu `APP_MODE="LIVE"` → dùng `AwsEC2Adapter(region_name)`.
    - Ngược lại dùng `MockEC2Adapter()`.
  - `get_ec2_repo()` được dùng trong `Depends` ở các route để inject repository.

- **Ports / Adapters (`modern_backend/ports` & `modern_backend/adapters`)**:
  - `IEC2Repository` / `AbstractEC2Repository` (trong `ports/repository.py`):
    - Định nghĩa interface bất đồng bộ: `list_instances`, `start_instance`, `stop_instance`, `terminate_instance`, `unit_economics`, `list_volumes`, `create_volume`, `attach_volume`, `detach_volume`, `enforce_rate_limit`, `list_events`, …
  - **Adapter AWS thật: `AwsEC2Adapter`**:
    - Dùng lại `ec2_manager.core.EC2Manager` để không viết lại logic AWS.
    - Vì `EC2Manager` là **đồng bộ**, adapter bọc các lời gọi trong `anyio.to_thread.run_sync` để không block event loop:
      - `list_instances()` → gọi `_mgr.list_instances(...)`, parse thành list `Instance` (Pydantic) bằng `Instance.parse_obj`.
      - `start_instance`, `stop_instance`, `terminate_instance()` → gọi hàm tương ứng, sau đó dựng lại một `Instance` tối thiểu với state mới.
      - `list_volumes()` → `Volume.parse_obj` list dict từ `list_volumes`.
      - `create_volume()` – dùng trực tiếp `self._mgr.ec2_client.create_volume(...)` rồi map sang dict chuẩn cho `Volume`.
      - `attach_volume` / `detach_volume` → wrap thẳng EC2Manager.
      - `unit_economics()` hiện tại trả **list rỗng** (chừa chỗ để sau này tính toán dựa trên CloudWatch/cost).
      - `list_events()` tạm thời trả rỗng.
  - **Adapter mock: `MockEC2Adapter`**:
    - Giữ state trong `_MockStateStore` in‑memory:
      - `instances`: hai instance mock (`i-mock-001` running, `i-mock-002` stopped).
      - `volumes`: `vol-mock-001` available, `vol-mock-002` in‑use gắn với `i-mock-001`.
      - `total_quota_gib`: 1024 (1 TB).
      - `events`: list `EventEntry` dùng như event log.
      - `metrics`: list `UnitEconomicsPoint` mô phỏng cost/CPU theo thời gian.
    - `list_instances()` – sleep 0.05s cho giống latency, trả snapshot hiện tại.
    - `start_instance()`:
      - Nếu chưa tồn tại → `ValueError`.
      - Nếu đang `running/pending` → trả lại như cũ.
      - Nếu `stopped` → chuyển sang `pending`, ghi event `"Start command received"`, sau đó tạo task async:
        - Sau 3–6 giây random:
          - 1% cơ hội chuyển sang state `impaired` + event error.
          - Ngược lại chuyển `running` + event info.
    - `stop_instance()` tương tự: chuyển từ `running` → `stopping` và sau 2–4 giây thành `stopped`, có event cảnh báo/info.
    - `terminate_instance()`:
      - Đổi state sang `shutting_down`, sau 1–2 giây thành `terminated` + event.
    - `unit_economics()`:
      - Nếu chưa có dữ liệu: sinh 24 điểm trong 24h qua, cost tăng dần theo số instance running & dung lượng volume; CPU utilization ≈ `running * 20%`.
      - Nếu đã có: thêm một điểm mới, cost tăng theo delta đơn giản, giới hạn lịch sử 48 điểm.
    - `list_volumes()`:
      - Chuẩn hoá field `AttachedInstances` dựa trên `InstanceId` nếu cần, rồi parse thành `Volume`.
    - `create_volume()`:
      - Nếu dùng lượng + size mới > quota → `RuntimeError("VolumeLimitExceeded...")`.
      - Ngược lại tạo `vol-mock-00X`, sleep 0.1s, ghi event `"Created mock volume"`.
    - `attach_volume()` / `detach_volume()`:
      - Cập nhật `State`, `InstanceId`, `AttachedInstances`, sleep 0.1s, ghi event log tương ứng.
    - `enforce_rate_limit()`:
      - 5% cơ hội ném `RuntimeError("RequestLimitExceeded...")` để mô phỏng throttling.
    - `list_events(limit)` – trả về các event gần nhất.

- **Domain models (`modern_backend/domain`)**:
  - `instance.py`:
    - `InstanceState` (Enum) cho mọi state EC2 chuẩn.
    - `Instance` (Pydantic) với alias khớp JSON từ AWS/EC2Manager (`AccountId`, `InstanceId`, `InstanceType`, `State`, `AvailabilityZone`, …).
    - `UnitEconomicsPoint` – một điểm thời gian gồm `timestamp`, `cost_per_hour`, `cpu_utilization`.
  - `volume.py`:
    - `Volume` với alias: `VolumeId`, `SizeGiB`, `State`, `VolumeType`, `AvailabilityZone`, `Throughput`, `Iops`, `AttachedInstances`.
  - `event.py`:
    - `EventLevel` (info, warning, error).
    - `EventEntry` – event log có `id`, `timestamp`, `level`, `message`, `context`.
  - `overview.py`:
    - `DashboardStats`: số instances, running, stopped, tổng volumes, volumes available, quota used/total.
    - `OverviewPayload`: payload cho `/api/v1/overview` gồm `mode`, `region`, `generated_at`, `stats`, `instances`, `volumes`, `events`, `metrics`.

- **API Routers tiêu biểu**:
  - `routes_instances.py`:
    - `GET /api/v1/instances` → trả `List[Instance]` từ `repo.list_instances()`.
    - `POST /api/v1/instances/{id}/start|stop|terminate` → gọi repo, map lỗi `ValueError` thành HTTP 404.
    - `GET /api/v1/economics` → trả `List[UnitEconomicsPoint]`.
    - `GET /api/v1/instances/throttled` → gọi `repo.enforce_rate_limit()`, nếu `RuntimeError` → 429.
  - `routes_volumes.py`:
    - `GET /api/v1/volumes` → list volumes.
    - `POST /api/v1/volumes` (body: `size_gib`, `availability_zone`) → tạo volume, bắt `RuntimeError` thành 429 (quota exceeded).
    - `POST /api/v1/volumes/attach|detach` → attach/detach, `ValueError` → 404.
  - `routes_overview.py`:
    - `GET /api/v1/overview`:
      - Gọi `repo.list_instances()`, `repo.list_volumes()`, `repo.list_events(limit=80)`, `repo.unit_economics()`.
      - Tính `running`, `stopped`, `total_gib`, `used_gib`.
      - Suy ra `mode = "mock"` nếu repo là `MockEC2Adapter` hoặc có `use_localstack`, ngược lại `"live"`.
      - Region lấy từ store hoặc `AWS_REGION`.
      - Kết hợp thành `OverviewPayload` – đây là **payload chính cho dashboard hiện đại**.

### 3. Frontend hiện đại: `modern_frontend`

- **Công nghệ**:
  - React 18 + TypeScript, Vite (`vite.config.ts`), Tailwind (`tailwind.config.ts`, `postcss.config.js`).
  - `@tanstack/react-query` để fetch/cache dữ liệu từ API.
  - `axios` làm HTTP client, `recharts` cho biểu đồ.

- **Cấu trúc chính**:
  - `src/lib/api.ts` & `src/lib/utils.ts`: config axios client (base URL, helpers), utils chung.
  - `src/ui/App.tsx` & `src/main.tsx`: bootstrapping React, router/layout chính, theme.
  - `src/components/ui/*`: button, card, badge, table – các building blocks UI.
  - `src/features/*`:
    - `dashboard`:
      - `api.ts` – hook/react-query gọi `/api/v1/overview`.
      - `UnitEconomicsChart.tsx` – biểu đồ cost/CPU dựa trên `metrics` từ backend.
      - `types.ts` – type TS mapping với `OverviewPayload`.
    - `instances`:
      - `api.ts` – endpoints để list/start/stop/terminate instances.
      - `InstanceTable.tsx` – bảng hiển thị trạng thái EC2 + actions.
      - `types.ts` – type TS mapping với `Instance`.
    - `volumes`:
      - `api.ts` – list/create/attach/detach volumes.
      - `VolumePanel.tsx` – UI quản lý volumes & quota.
      - `types.ts` – type TS cho `Volume`.
    - `events`:
      - `api.ts` – fetch event log.
      - `EventLog.tsx` – bảng log realtime (thường poll định kỳ).

- **Luồng hoạt động với backend**:
  - FE gọi các API của `modern_backend`:
    - `/api/v1/overview` để render dashboard tổng quan (stats, charts).
    - `/api/v1/instances` + actions start/stop/terminate.
    - `/api/v1/volumes` + create/attach/detach.
    - `/api/v1/events` (nếu có) cho log.
  - Tuỳ giá trị `APP_MODE` trong backend:
    - **MOCK**: dữ liệu demo từ `MockEC2Adapter` – an toàn, không cần AWS.
    - **LIVE**: thật trên AWS qua `AwsEC2Adapter` + `EC2Manager`.

---

## Các chế độ chạy & cách hoạt động tổng quát

- **CLI (`ec2-man`)**:
  - Nhận lệnh từ người dùng (instance/volume/report).
  - Tạo `EC2Manager` với region/profile phù hợp.
  - Gọi các hàm EC2/CloudWatch thông qua `boto3`, xử lý lỗi, in JSON/CSV ra stdout.
  - Phù hợp cho automation, script, export inventory/cost.

- **Web cũ (Flask/FastAPI trong `ec2_manager`)**:
  - Cung cấp giao diện HTML bootstrap/Jinja terminal‑style:
    - Xem & điều khiển instance/volume.
    - Xem inventory & cost optimization.
  - Một số endpoint JSON để terminal/JS client khác sử dụng.
  - Có tuỳ chọn Basic Auth rất đơn giản.

- **Kiến trúc modern (FastAPI + React)**:
  - Backend FastAPI (`modern_backend`) đóng vai trò **BFF / API layer** hiện đại:
    - Thu dữ liệu từ AWS thật hoặc từ mock store.
    - Chuẩn hoá sang các model domain và payload gọn cho UI.
    - Cài đặt logic nhỏ về quota, rate limit, event log, unit economics.
  - Frontend React (`modern_frontend`) đóng vai trò **Dashboard UI**:
    - Hiển thị card thống kê (số lượng instance/volume, quota used, mode = mock/live).
    - Hiển thị bảng instance & volume với action real‑time.
    - Biểu đồ chi phí & CPU theo thời gian, log event hệ thống.

---

## Mock mode, LocalStack & môi trường phát triển

- **Mock mode (không cần AWS)**:
  - Được mô tả trong `README.md` và `run_mock_mode.py`.
  - Backend modern sử dụng `APP_MODE=MOCK` (mặc định), `MockEC2Adapter` là nguồn dữ liệu.
  - Cho phép chạy dashboard/CLI demo mà không tạo chi phí thực tế.

- **LocalStack**:
  - `EC2Manager` hỗ trợ endpoint `LOCALSTACK_ENDPOINT` / `AWS_ENDPOINT_URL`.
  - Tự thiết lập credential giả `"test"` nếu dùng LocalStack.
  - Cho phép test luồng thật với API AWS mà không đụng cloud thật.

- **AWS thật**:
  - Yêu cầu cấu hình credentials (AWS CLI, env vars, profile, IAM Role).
  - Cần IAM policy với các quyền EC2/CloudWatch được liệt kê trong `README.md`.

---

## Tóm tắt ngắn gọn

- **Dự án này là một hệ sinh thái quản lý EC2/EBS đa kênh**:
  - **CLI** mạnh cho automation và scripting.
  - **Web UI cũ (Flask/FastAPI)** cho giao diện nhanh, tái dùng trực tiếp `EC2Manager`.
  - **Kiến trúc mới (FastAPI async + React/Tailwind)** để xây dựng trải nghiệm dashboard hiện đại, mở rộng được, có mock mode và adapter AWS tách biệt.
- “**Bộ máy chính**” là lớp **`EC2Manager`** (core AWS logic) cùng với các **adapter repository** (`AwsEC2Adapter`, `MockEC2Adapter`) dùng trong backend hiện đại để phơi bày dữ liệu/operation EC2/EBS dưới dạng API sạch cho UI và các client khác.



