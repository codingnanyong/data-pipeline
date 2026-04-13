## `pipeline/rdb_extract`

다수의 RDB(Postgres/Timescale 등)에서 **확장 가능(config-driven)** 하게 데이터를 추출(Extract)하는 DAG 예시 모듈입니다.

- `*_config.py`: 소스 DB/테이블 선언만 추가하면 확장
- `*_tasks.py`: 실제 추출 로직(공통)
- `*_utils.py`: Variable 키/로컬 경로 등 공통 유틸
- `rdb_extract_dag.py`: Incremental/Backfill DAG 정의(동적 TaskGroup)

