# OAI-PMH Paper Harvester

OAI-PMH에서 공개되는 메타데이터를 수집해 Snowflake에 적재하는 파이썬 수집기입니다.

## 특징

- OAI-PMH `ListRecords`를 사용한 증분 수집
- `resumptionToken` 기반 페이지네이션 처리
- 중복 식별자 중 최신 datestamp 기반 dedupe
- 오픈 액세스 판별 후 저장 필터링 (옵션)
- Snowflake `MERGE` 업서트
- 로컬 상태 파일 기반 재개 지원

## 빠른 실행

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]

export OAI_BASE_URL="https://export.arxiv.org/oai2"
export OAI_METADATA_PREFIX="oai_dc"
export SNOWFLAKE_ACCOUNT="<account>"
export SNOWFLAKE_USER="<user>"
export SNOWFLAKE_PASSWORD="<password>"
export SNOWFLAKE_DATABASE="HARMONIA"
export SNOWFLAKE_SCHEMA="PUBLIC"
export SNOWFLAKE_TABLE="PAPERS"

oai-pmh-harvester
```

드라이런:

```bash
oai-pmh-harvester --dry-run
```

## 환경 변수

| 이름 | 설명 | 기본값 |
| --- | --- | --- |
| `OAI_BASE_URL` | OAI-PMH 엔드포인트 URL | `required` |
| `OAI_METADATA_PREFIX` | 메타데이터 포맷 | `oai_dc` |
| `OAI_SET` | set 매개변수 | 없음 |
| `OAI_FROM` | UTC datetime/date | 없음 |
| `OAI_UNTIL` | UTC datetime/date | 없음 |
| `OPEN_ACCESS_ONLY` | 오픈 액세스만 저장 (`true`/`false`) | `false` |
| `OPEN_ACCESS_TERMS` | 오픈 액세스 판별 키워드 (`,` 구분) | 기본 내장 |
| `HARVEST_BATCH_SIZE` | 업서트 배치 크기 (`0`이면 전체 일괄) | `500` |
| `OAI_REQUEST_TIMEOUT` | API 요청 타임아웃(초) | `30` |
| `OAI_USER_AGENT` | 요청 User-Agent 문자열 | `oai-pmh-scraper/0.1.0` |
| `OAI_STATE_FILE` | 상태 파일 경로 | `.oai_harvest_state.json` |

Snowflake 인증:

| 이름 | 설명 | 기본값 |
| --- | --- | --- |
| `SNOWFLAKE_ACCOUNT` | Snowflake 계정 | - |
| `SNOWFLAKE_USER` | 사용자 | - |
| `SNOWFLAKE_PASSWORD` | 비밀번호 | - |
| `SNOWFLAKE_ROLE` | 역할 | - |
| `SNOWFLAKE_WAREHOUSE` | 웨어하우스 | - |
| `SNOWFLAKE_DATABASE` | DB | `HARMONIA` |
| `SNOWFLAKE_SCHEMA` | SCHEMA | `PUBLIC` |
| `SNOWFLAKE_TABLE` | 테이블 | `PAPERS` |

## 라이선스

MIT
Tooling to harvest open-access papers via OAI-PMH and store in Snowflake
