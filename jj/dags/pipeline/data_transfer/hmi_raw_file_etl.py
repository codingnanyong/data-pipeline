"""
HMI Raw Data Archive DAG
========================
HMI 장비의 RAW DATA 파일(.csv)을 다운로드하는 DAG

목적:
- 파일명에서 날짜를 추출하여 지정된 날짜 범위의 파일만 수집
- 파일명 형식: CBM_192.168.8.51_2025112607_T1_ST1.csv

Pipeline Flow:
1. HMI 호스트 연결 확인
2. 지정된 날짜 범위의 RAW DATA 파일 목록 조회 (.csv)
3. 파일 다운로드 (로컬 백업)
4. 파일 검증
5. 완료 보고

DAG 종류:
- Incremental: 매일 실행, 전일 파일만 수집
- Backfill: 수동 실행, 과거부터 오늘-2일까지 수집
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from pipeline.data_transfer.hmi_raw_file_etl_config import (
    DEFAULT_ARGS,
    HMI_CONFIGS,
    INITIAL_START_DATE,
    DAYS_OFFSET_FOR_BACKFILL,
    HOURS_OFFSET_FOR_HOURLY,
    INDO_TZ,
)

def get_configs_for_dag(dag_id: str):
    """DAG 종류에 따라 사용할 HMI 설정 리스트 반환.
    - Backfill: backfill_enabled=True인 것만
    - Incremental/Hourly: incremental_enabled=True인 것만 (미설정 시 True)
    """
    if not dag_id:
        return list(HMI_CONFIGS)
    if dag_id.endswith("_backfill"):
        return [c for c in HMI_CONFIGS if c.get("backfill_enabled", True)]
    if dag_id.endswith("_incremental") or dag_id.endswith("_hourly"):
        return [c for c in HMI_CONFIGS if c.get("incremental_enabled", True)]
    return list(HMI_CONFIGS)

from pipeline.data_transfer.hmi_raw_file_etl_tasks import (
    test_connection,
    list_remote_files,
    download_files,
    generate_summary_report,
    update_variable_after_run,
    cleanup_old_remote_files,
)

# ════════════════════════════════════════════════════════════════
# Helper Functions for DAGs
# ════════════════════════════════════════════════════════════════

def prepare_date_range(**kwargs) -> dict:
    """
    날짜 범위 준비 (Incremental/Hourly용)
    start_date는 각 HMI별 Variable(last_extract_time_...)에서 읽고, 여기서는 end_date만 반환.
    인도네시아 시간 기준 현재 시간 - 1시간까지 수집.
    """
    # 인도네시아 시간 기준 현재 시간
    now_indo = datetime.now(INDO_TZ)
    # 현재 시간 - 1시간까지 (예: 14시 실행 시 13시까지)
    end_date = now_indo - timedelta(hours=HOURS_OFFSET_FOR_HOURLY)
    # 분, 초, 마이크로초 제거 (시간 단위로만 처리)
    end_date = end_date.replace(minute=0, second=0, microsecond=0)
    
    logging.info(f"📅 Hourly 공통 종료 시간 (인도네시아): {end_date.strftime('%Y-%m-%d %H:%M')} 이전")
    logging.info(f"   (각 HMI별 시작 시간은 Variable에서 읽습니다)")
    
    return {
        "start_date": None,  # 각 HMI별로 Variable에서 읽음
        "end_date": end_date.strftime('%Y-%m-%d %H:%M') if end_date else None  # 날짜+시간 저장
    }


def prepare_backfill_date_range(**kwargs) -> dict:
    """
    날짜 범위 준비 (Backfill용)
    config의 INITIAL_START_DATE ~ 오늘-2일.
    - 필터링: 하루 단위 범위
    - Variable: hourly와 동일하게 'YYYY-MM-DD HH:MM' 형식으로 저장
    """
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = INITIAL_START_DATE.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = today - timedelta(days=DAYS_OFFSET_FOR_BACKFILL)

    # 로그는 날짜+시간까지 표시
    logging.info(
        f"📅 Backfill 날짜 범위: "
        f"{start_date.strftime('%Y-%m-%d %H:%M')} ~ {end_date.strftime('%Y-%m-%d 23:59')} 까지"
    )
    
    # Variable / XCom에 저장할 때도 hourly와 동일하게 'YYYY-MM-DD HH:MM' 형식 사용
    # - start_date: 00:00
    # - end_date:   23:59 (해당 날짜의 마지막까지 수집했다는 의미)
    return {
        "start_date": start_date.strftime('%Y-%m-%d %H:%M') if start_date else None,
        "end_date": end_date.replace(hour=23, minute=59).strftime('%Y-%m-%d %H:%M') if end_date else None,
    }


def create_hmi_tasks(dag, date_range_func):
    """
    HMI별 태스크 생성 (공통 함수)
    
    Args:
        dag: DAG 객체
        date_range_func: 날짜 범위를 준비하는 함수
    
    Returns:
        (prepare_date_range_task, first_tasks, last_tasks, final_task): 
        - prepare_date_range_task: 날짜 범위 준비 태스크 (공통, list_task에서 XCom으로 사용)
        - first_tasks: 모든 HMI의 첫 번째 태스크 리스트 (병렬 실행)
        - last_tasks: 모든 HMI의 마지막 태스크 리스트 (모두 완료 후 summary)
        - final_task: 마지막 태스크 (요약 보고서 또는 Variable 업데이트)
    """
    # 날짜 범위 준비 태스크
    prepare_date_range_task = PythonOperator(
        task_id="prepare_date_range",
        python_callable=date_range_func,
        dag=dag,
    )
    
    # 각 HMI별로 태스크 생성 (Backfill DAG는 backfill_enabled=True인 설정만 사용)
    configs_to_use = get_configs_for_dag(dag.dag_id)
    if not configs_to_use:
        logging.warning(
            "⚠️ 사용할 HMI 설정이 없습니다. "
            "Backfill: backfill_enabled=True, Incremental/Hourly: incremental_enabled=True인 항목이 있는지 확인하세요."
        )
    hmi_tasks = {}

    for hmi_config in configs_to_use:
        hmi_id = hmi_config['hmi_id']
        
        # HMI별 태스크 ID 생성
        test_task_id = f"test_connection_{hmi_id}"
        list_task_id = f"list_remote_files_{hmi_id}"
        download_task_id = f"download_files_{hmi_id}"
        
        # Task 1: 연결 테스트
        test_task = PythonOperator(
            task_id=test_task_id,
            python_callable=test_connection,
            op_kwargs={"hmi_config": hmi_config},
            dag=dag,
        )
        
        # Task 2: 원격 파일 목록 조회 (클로저 문제 방지를 위해 기본 인자 사용)
        def create_list_callable(config, is_incremental):
            def list_with_date_range(**kwargs):
                ti = kwargs['ti']
                date_range = ti.xcom_pull(task_ids='prepare_date_range')
                start_date = None
                end_date = None
                
                if date_range:
                    # 날짜 파싱 (ISO 형식, 'YYYY-MM-DD' 형식, 'YYYY-MM-DD HH:MM' 형식 모두 지원)
                    def parse_date(date_str):
                        """유연한 날짜 파싱: ISO 형식, 'YYYY-MM-DD' 형식, 'YYYY-MM-DD HH:MM' 형식"""
                        if not date_str:
                            return None
                        try:
                            # ISO 형식 시도 (예: '2025-11-26T00:00:00')
                            from dateutil.parser import parse
                            parsed = parse(date_str)
                            # 시간대가 없으면 인도네시아 시간대로 설정
                            if parsed.tzinfo is None:
                                parsed = parsed.replace(tzinfo=INDO_TZ)
                            return parsed
                        except (ValueError, TypeError):
                            try:
                                # 'YYYY-MM-DD HH:MM' 형식 시도 (hourly용)
                                parsed = datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                                return parsed.replace(tzinfo=INDO_TZ)
                            except ValueError:
                                try:
                                    # 'YYYY-MM-DD' 형식 시도 (daily용)
                                    parsed = datetime.strptime(date_str, '%Y-%m-%d')
                                    return parsed.replace(tzinfo=INDO_TZ)
                                except ValueError:
                                    logging.warning(f"날짜 파싱 실패: {date_str}")
                                    return None
                    
                    if date_range.get('start_date'):
                        start_date = parse_date(date_range['start_date'])
                    if date_range.get('end_date'):
                        end_date = parse_date(date_range['end_date'])
                
                # Backfill: start_date/end_date는 위 date_range(XCom)만 사용(init 기준). Incremental: start_date는 Variable에서 조회.
                return list_remote_files(
                    hmi_config=config,
                    start_date=start_date,
                    end_date=end_date,
                    use_variable=is_incremental,
                    **kwargs
                )
            return list_with_date_range
        
        # Incremental DAG인지 확인 (hourly도 incremental로 처리)
        is_incremental = dag.dag_id.endswith('_incremental') or dag.dag_id.endswith('_hourly')
        # Backfill DAG에서는 원격 cleanup 사용 안 함 (과거 데이터 수집만 수행)
        use_cleanup = is_incremental

        list_task = PythonOperator(
            task_id=list_task_id,
            python_callable=create_list_callable(hmi_config, is_incremental),
            dag=dag,
        )

        # Task 3: 원격 SFTP 오래된 파일 정리 (Incremental/Hourly만, Backfill 제외)
        if use_cleanup:
            cleanup_task = PythonOperator(
                task_id=f"cleanup_old_files_{hmi_id}",
                python_callable=cleanup_old_remote_files,
                op_kwargs={"hmi_config": hmi_config},
                dag=dag,
            )

        # Task 4: 파일 다운로드
        download_task = PythonOperator(
            task_id=download_task_id,
            python_callable=download_files,
            op_kwargs={"hmi_config": hmi_config},
            dag=dag,
        )

        # HMI별 태스크 의존성: list → (cleanup 있으면 cleanup →) download
        if use_cleanup:
            test_task >> list_task >> cleanup_task >> download_task
            hmi_tasks[hmi_id] = {"test": test_task, "list": list_task, "download": download_task, "cleanup": cleanup_task}
        else:
            test_task >> list_task >> download_task
            hmi_tasks[hmi_id] = {"test": test_task, "list": list_task, "download": download_task}
    
    # 전체 요약 보고서 (모든 HMI 처리 완료 후)
    generate_summary_task = PythonOperator(
        task_id="generate_summary_report",
        python_callable=generate_summary_report,
        dag=dag,
    )
    
    # Variable 업데이트 (Incremental/Hourly/Backfill DAG에서 모두 사용)
    update_var_task = None
    if dag.dag_id.endswith('_incremental') or dag.dag_id.endswith('_hourly') or dag.dag_id.endswith('_backfill'):
        update_var_task = PythonOperator(
            task_id="update_variable",
            python_callable=update_variable_after_run,
            dag=dag,
        )
    
    # 모든 HMI의 download 태스크가 완료된 후 요약 보고서 생성
    for hmi_id, tasks in hmi_tasks.items():
        tasks["download"] >> generate_summary_task
    
    # Incremental 또는 Hourly DAG인 경우 Variable 업데이트 추가
    final_task = update_var_task if update_var_task else generate_summary_task
    if update_var_task:
        generate_summary_task >> update_var_task
    
    # 모든 HMI의 첫 번째 태스크 리스트 (병렬 실행용)
    first_tasks = [tasks["test"] for tasks in hmi_tasks.values()]
    # 모든 HMI의 마지막 태스크 리스트 (summary 전에 모두 완료되어야 함)
    last_tasks = [tasks["download"] for tasks in hmi_tasks.values()]
    
    return prepare_date_range_task, first_tasks, last_tasks, final_task


# ════════════════════════════════════════════════════════════════
# DAG Definitions
# ════════════════════════════════════════════════════════════════

# ────────────────────────────────────────────────────────────────
# Incremental DAG: 매시간 실행, 현재 시간 -1시간까지 파일 수집 (인도네시아 시간 기준)
# ────────────────────────────────────────────────────────────────

with DAG(
    dag_id="hmi_raw_file_etl_incremental",
    default_args=DEFAULT_ARGS,
    description="HMI RAW DATA 수집 (Incremental): 매시간 현재 시간 -1시간까지 파일 수집 (인도네시아 시간 기준)",
    schedule_interval="@hourly",  # 매시간 0분 실행
    catchup=False,
    tags=["hmi", "data_transfer", "file", "incremental"],
    max_active_runs=1,
) as incremental_dag:
    
    start = DummyOperator(task_id='start', dag=incremental_dag)
    end = DummyOperator(task_id='end', dag=incremental_dag)
    
    prepare_date_range_task, first_tasks, last_tasks, final_task = create_hmi_tasks(incremental_dag, prepare_date_range)
    
    # 병렬 실행 구조: start -> prepare_date_range -> [각 HMI 병렬] -> summary -> update_var -> end
    start >> prepare_date_range_task  # 공통 날짜 범위 준비
    prepare_date_range_task >> first_tasks  # 각 HMI 병렬 시작 (리스트로 분기)
    last_tasks >> final_task  # 모든 HMI 완료 후 최종 처리 (리스트에서 합류)
    final_task >> end

# ────────────────────────────────────────────────────────────────
# Backfill DAG: 수동 실행, 과거부터 오늘-2일까지 수집
# ────────────────────────────────────────────────────────────────

with DAG(
    dag_id="hmi_raw_file_etl_backfill",
    default_args=DEFAULT_ARGS,
    description="HMI RAW DATA 수집 (Backfill): 과거부터 오늘-2일까지 수집",
    schedule_interval=None,  # 수동 실행
    catchup=False,
    tags=["hmi", "data_transfer", "file", "backfill"],
    max_active_runs=1,
) as backfill_dag:
    
    start = DummyOperator(task_id='start', dag=backfill_dag)
    end = DummyOperator(task_id='end', dag=backfill_dag)
    
    prepare_date_range_task, first_tasks, last_tasks, final_task = create_hmi_tasks(backfill_dag, prepare_backfill_date_range)
    
    # 병렬 실행 구조: start -> prepare_date_range -> [각 HMI 병렬] -> summary -> end
    start >> prepare_date_range_task  # 공통 날짜 범위 준비
    prepare_date_range_task >> first_tasks  # 각 HMI 병렬 시작 (리스트로 분기)
    last_tasks >> final_task  # 모든 HMI 완료 후 최종 처리 (리스트에서 합류)
    final_task >> end
