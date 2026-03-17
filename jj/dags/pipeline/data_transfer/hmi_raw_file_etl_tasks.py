"""
HMI Raw Data ETL Tasks
======================
Airflow Task 함수들: 연결 테스트, 파일 목록 조회, 다운로드, 요약 보고서 등
"""

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import fnmatch

from airflow.exceptions import AirflowSkipException
from airflow.models import Connection, Variable
from airflow.providers.sftp.hooks.sftp import SFTPHook

from pipeline.data_transfer.hmi_raw_file_etl_config import (
    HMI_CONFIGS,
    REMOTE_FILE_PATTERNS,
    VERIFY_FILE_SIZE,
    LARGE_FILE_THRESHOLD_MB,
    DAYS_OFFSET_FOR_INCREMENTAL,
)
from pipeline.data_transfer.hmi_raw_file_etl_utils import (
    get_variable_key,
    format_date_filter_desc,
    normalize_remote_path,
    filter_remote_files,
    ensure_local_directory,
    check_disk_space,
)


def get_date_range_from_variable(hmi_config: dict, is_hourly: bool = False) -> tuple[Optional[datetime], Optional[datetime]]:
    """Variable에서 날짜 범위 읽기 (날짜만 저장된 기존 값도 처리)"""
    from pipeline.data_transfer.hmi_raw_file_etl_config import INDO_TZ
    
    hmi_id = hmi_config['hmi_id']
    variable_key = get_variable_key(hmi_config)
    last_extract_time = Variable.get(variable_key, default_var=None)
    
    if last_extract_time:
        # 날짜 형식 판단: 'YYYY-MM-DD' 형식인지 확인
        is_date_only = len(last_extract_time.strip()) == 10 and last_extract_time.count('-') == 2
        
        if is_date_only:
            # 날짜만 있는 경우 (예: "2025-11-27")
            try:
                parsed_date = datetime.strptime(last_extract_time, '%Y-%m-%d')
                parsed_date = parsed_date.replace(tzinfo=INDO_TZ)
                if is_hourly:
                    # Hourly: 해당 날짜의 00:00부터 시작
                    start_date = parsed_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    logging.info(f"📌 [{hmi_id}] 마지막 처리 시간 (Variable, 날짜만): {last_extract_time} → 시작: {start_date.strftime('%Y-%m-%d %H:%M')}")
                else:
                    # Daily: 날짜 단위로 처리
                    start_date = parsed_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    logging.info(f"📌 [{hmi_id}] 마지막 처리 시간 (Variable): {last_extract_time} → 시작: {start_date.strftime('%Y-%m-%d')}")
                return start_date, None
            except ValueError:
                logging.warning(f"⚠️ [{hmi_id}] Variable 날짜 파싱 실패: {last_extract_time}")
                return None, None
        else:
            # 날짜+시간 형식 또는 ISO 형식
            from dateutil.parser import parse
            parsed_date = parse(last_extract_time)
            
            # 시간대가 없으면 인도네시아 시간대로 설정
            if parsed_date.tzinfo is None:
                parsed_date = parsed_date.replace(tzinfo=INDO_TZ)
            
            if is_hourly:
                # Hourly: 시간 단위로 처리 (분, 초, 마이크로초 제거)
                start_date = parsed_date.replace(minute=0, second=0, microsecond=0)
                logging.info(f"📌 [{hmi_id}] 마지막 처리 시간 (Variable): {last_extract_time} → 시작: {start_date.strftime('%Y-%m-%d %H:%M')}")
            else:
                # Daily: 날짜 단위로 처리
                start_date = parsed_date.replace(hour=0, minute=0, second=0, microsecond=0)
                logging.info(f"📌 [{hmi_id}] 마지막 처리 시간 (Variable): {last_extract_time} → 시작: {start_date.strftime('%Y-%m-%d')}")
            
            return start_date, None
    else:
        if is_hourly:
            logging.warning(f"⚠️ [{hmi_id}] Variable '{variable_key}' not found. Using default: 현재 시간 -1시간까지")
        else:
            logging.warning(f"⚠️ [{hmi_id}] Variable '{variable_key}' not found. Using default: 전일까지")
        return None, None


def test_connection(hmi_config: dict, **kwargs) -> dict:
    """외부 호스트 연결 테스트 (HMI별)"""
    sftp_conn_id = hmi_config['sftp_conn_id']
    hmi_id = hmi_config['hmi_id']
    
    logging.info(f"🔌 [{hmi_id}] 외부 호스트 연결 테스트 시작: {sftp_conn_id}")
    
    # Connection 존재 여부 확인
    try:
        conn = Connection.get_connection_from_secrets(sftp_conn_id)
        if not conn:
            raise ValueError(f"Connection '{sftp_conn_id}' not found in Airflow Connections")
        logging.info(f"📋 [{hmi_id}] Connection 정보 확인: host={conn.host}, port={conn.port}, login={conn.login}")
    except Exception as e:
        error_msg = (
            f"❌ [{hmi_id}] Connection '{sftp_conn_id}' 확인 실패: {e}\n"
            f"해결 방법:\n"
            f"  1. Airflow UI → Admin → Connections에서 '{sftp_conn_id}' Connection이 존재하는지 확인\n"
            f"  2. Connection Type이 'SFTP'인지 확인\n"
            f"  3. Host, Port, Login, Password가 올바르게 설정되었는지 확인"
        )
        logging.error(error_msg)
        raise ValueError(error_msg) from e
    
    # SFTP 연결 시도 (재시도 로직 포함)
    max_retries = 5
    retry_delay = 10  # 초 (서버 측 문제 대비)
    
    for attempt in range(1, max_retries + 1):
        sftp_hook = None
        try:
            logging.info(f"🔄 [{hmi_id}] SFTP 연결 시도 {attempt}/{max_retries}")
            sftp_hook = SFTPHook(ftp_conn_id=sftp_conn_id)
            
            # 연결 시도
            with sftp_hook.get_conn() as sftp:
                current_dir = sftp.getcwd()
                logging.info(f"✅ [{hmi_id}] 연결 성공! 현재 디렉토리: {current_dir}")
                
                # 연결 정보 반환
                result = {
                    "status": "success",
                    "hmi_id": hmi_id,
                    "current_dir": current_dir,
                    "connection_id": sftp_conn_id
                }
            
            # with 블록 종료 후 명시적으로 연결 종료
            if sftp_hook:
                try:
                    sftp_hook.close_conn()
                    logging.debug(f"🔌 [{hmi_id}] SFTP 연결 명시적으로 종료")
                except Exception as close_err:
                    logging.warning(f"⚠️ [{hmi_id}] SFTP 연결 종료 중 경고: {close_err}")
            
            return result
        except Exception as e:
            error_str = str(e)
            error_lower = error_str.lower()
            error_type = type(e).__name__
            
            # 오류 유형 분류 (더 포괄적으로)
            is_banner_error = "banner" in error_lower or "protocol" in error_lower
            is_auth_error = "authentication" in error_lower or "password" in error_lower or "auth" in error_lower
            is_reset_error = (
                "connection reset" in error_lower or 
                "errno 104" in error_lower or 
                "reset by peer" in error_lower or
                "connectionreset" in error_lower
            )
            is_timeout_error = "timeout" in error_lower or "timed out" in error_lower
            is_eof_error = "eof" in error_lower or error_type == "EOFError"
            is_ssh_error = "sshexception" in error_lower or error_type == "SSHException"
            is_connection_error = (
                "connection" in error_lower or 
                "network" in error_lower or
                error_type in ("ConnectionError", "ConnectionResetError", "OSError", "IOError") or
                is_reset_error or is_timeout_error or is_banner_error or is_eof_error or is_ssh_error
            )
            
            # 재시도 가능한 오류인지 확인 (연결 관련 오류 모두 재시도)
            is_retryable = is_connection_error and not is_auth_error
            
            # 연결 종료 시도
            if sftp_hook:
                try:
                    sftp_hook.close_conn()
                    logging.debug(f"🔌 [{hmi_id}] 예외 발생 후 SFTP 연결 종료 시도")
                except Exception as close_err:
                    logging.debug(f"⚠️ [{hmi_id}] 연결 종료 중 오류 (무시): {close_err}")
            
            if attempt < max_retries and is_retryable:
                logging.warning(
                    f"⚠️ [{hmi_id}] SFTP 연결 실패 (시도 {attempt}/{max_retries}): {error_str}\n"
                    f"   {retry_delay}초 후 재시도..."
                )
                time.sleep(retry_delay)
                continue
            elif attempt < max_retries and not is_retryable:
                # 재시도 불가능한 오류 (인증 오류 등)는 즉시 실패
                logging.error(f"❌ [{hmi_id}] SFTP 연결 실패 (재시도 불가): {error_str}")
                raise
            else:
                # 최종 실패 시 - 장비가 꺼져있거나 연결 불가능한 경우 Skip 처리
                # 인증 오류는 설정 문제일 수 있으므로 예외를 raise하여 DAG 실패 처리
                if is_auth_error:
                    error_msg = (
                        f"❌ [{hmi_id}] SFTP 인증 실패\n"
                        f"Connection ID: {sftp_conn_id}\n"
                        f"원인: {error_str}\n"
                        f"해결 방법:\n"
                        f"  - Airflow UI → Admin → Connections에서 '{sftp_conn_id}' Connection의\n"
                        f"    사용자명(Login)과 비밀번호(Password)를 확인하세요"
                    )
                    logging.error(error_msg)
                    raise ConnectionError(error_msg) from e
                
                # 그 외 연결 실패는 장비 전원 문제 등으로 간주하고 Skip
                # Skip 전에 Variable 업데이트 (연결 불가 시에도 다음 실행 시 올바른 시점부터 재시도)
                try:
                    from pipeline.data_transfer.hmi_raw_file_etl_config import INDO_TZ, HOURS_OFFSET_FOR_HOURLY, DAYS_OFFSET_FOR_INCREMENTAL
                    
                    # prepare_date_range에서 end_date 가져오기 또는 직접 계산
                    ti = kwargs.get('ti')
                    end_date_str = None
                    dag = kwargs.get('dag')
                    is_hourly = dag and (dag.dag_id.endswith('_hourly') or dag.dag_id.endswith('_incremental')) if dag else False
                    
                    if ti:
                        try:
                            date_range = ti.xcom_pull(task_ids='prepare_date_range')
                            if date_range and date_range.get('end_date'):
                                end_date_str = date_range.get('end_date')
                        except Exception as xcom_err:
                            logging.debug(f"[{hmi_id}] prepare_date_range XCom 읽기 실패: {xcom_err}")
                    
                    # XCom에서 가져오지 못한 경우 직접 계산
                    if not end_date_str:
                        if is_hourly:
                            now_indo = datetime.now(INDO_TZ)
                            end_date_dt = now_indo - timedelta(hours=HOURS_OFFSET_FOR_HOURLY)
                            end_date_dt = end_date_dt.replace(minute=0, second=0, microsecond=0)
                            end_date_str = end_date_dt.strftime('%Y-%m-%d %H:%M')
                        else:
                            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                            end_date_dt = today - timedelta(days=DAYS_OFFSET_FOR_INCREMENTAL)
                            end_date_str = end_date_dt.strftime('%Y-%m-%d')
                    
                    # Variable 업데이트
                    if end_date_str:
                        variable_key = get_variable_key(hmi_config)
                        Variable.set(variable_key, end_date_str)
                        logging.info(f"✅ [{hmi_id}] Variable '{variable_key}' 업데이트 (연결 불가로 Skip): {end_date_str}")
                except Exception as var_err:
                    logging.warning(f"⚠️ [{hmi_id}] Variable 업데이트 실패 (무시하고 계속): {var_err}")
                
                skip_msg = (
                    f"⏭️ [{hmi_id}] SFTP 연결 불가 (최대 재시도 횟수 초과) - 태스크 Skip\n"
                    f"Connection ID: {sftp_conn_id}\n"
                    f"시도 횟수: {max_retries}회\n"
                    f"최종 오류: {error_str}\n"
                    f"설명: 장비가 전원이 꺼져있거나 네트워크 연결이 불가능합니다.\n"
                    f"      다음 스케줄 실행 시 자동으로 재시도됩니다."
                )
                
                if is_reset_error:
                    skip_msg += (
                        f"\n가능한 원인:\n"
                        f"  - 장비 전원이 꺼져있음\n"
                        f"  - 네트워크 연결 불안정\n"
                        f"  - 방화벽/보안 정책으로 인한 연결 차단"
                    )
                elif is_banner_error or is_eof_error:
                    skip_msg += (
                        f"\n가능한 원인:\n"
                        f"  - 장비 전원이 꺼져있음\n"
                        f"  - 네트워크 연결 불안정\n"
                        f"  - SSH 서버 응답 지연 또는 비정상 종료"
                    )
                elif is_timeout_error:
                    skip_msg += (
                        f"\n가능한 원인:\n"
                        f"  - 장비 전원이 꺼져있음\n"
                        f"  - 네트워크 지연 또는 불안정\n"
                        f"  - 서버 응답 지연"
                    )
                else:
                    skip_msg += (
                        f"\n가능한 원인:\n"
                        f"  - 장비 전원이 꺼져있음\n"
                        f"  - 네트워크 연결 문제\n"
                        f"  - 호스트/포트 접근 불가"
                    )
                
                logging.warning(skip_msg)
                raise AirflowSkipException(skip_msg) from e


def list_remote_files(
    hmi_config: dict,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    use_variable: bool = True,
    **kwargs
) -> dict:
    """원격 호스트에서 지정된 날짜 범위의 RAW DATA 파일 목록 조회 (HMI별)"""
    from pipeline.data_transfer.hmi_raw_file_etl_config import INDO_TZ, DAYS_OFFSET_FOR_INCREMENTAL, HOURS_OFFSET_FOR_HOURLY
    
    sftp_conn_id = hmi_config['sftp_conn_id']
    remote_base_path = normalize_remote_path(hmi_config['remote_base_path'])
    hmi_id = hmi_config['hmi_id']
    
    # DAG ID 확인하여 hourly 여부 판단 (incremental도 매시간 실행하므로 hourly로 처리)
    dag = kwargs.get('dag')
    is_hourly = dag and (dag.dag_id.endswith('_hourly') or dag.dag_id.endswith('_incremental')) if dag else False

    # Backfill: start_date/end_date는 XCom만 사용 (prepare_backfill_date_range → INITIAL_START_DATE ~ 오늘-2일). Variable 미사용.
    # Incremental: start_date는 Variable(last_extract_time_...) 기준, end_date는 XCom(현재-1시간 등) 기준.
    if use_variable:
        # Incremental: Variable에서 마지막 처리 시점을 읽어 start_date로 사용
        if start_date is None:
            start_date, _ = get_date_range_from_variable(hmi_config, is_hourly=is_hourly)
    # (Backfill이면 use_variable=False → 위 분기 진입 안 함, 전달된 start_date/end_date 그대로 사용)

    # end_date 설정 (Incremental에서만; end_date가 None이면 현재-1시간 등으로 계산)
    if use_variable and end_date is None:
        if is_hourly:
            # Hourly: 인도네시아 시간 기준 현재 시간 - 1시간
            now_indo = datetime.now(INDO_TZ)
            end_date = now_indo - timedelta(hours=HOURS_OFFSET_FOR_HOURLY)
            end_date = end_date.replace(minute=0, second=0, microsecond=0)
        else:
            # Daily: 전일까지
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = today - timedelta(days=DAYS_OFFSET_FOR_INCREMENTAL)

    # Incremental에서 Variable이 미래 날짜인 경우 (start_date > end_date): 수집 구간이 비어 있으므로 start_date 제거
    if use_variable and start_date is not None and end_date is not None and start_date > end_date:
        logging.warning(
            f"⚠️ [{hmi_id}] Variable 시작 시간이 종료 시간보다 미래입니다 "
            f"(start_date={start_date.strftime('%Y-%m-%d %H:%M')}, end_date={end_date.strftime('%Y-%m-%d %H:%M')}). "
            "start_date를 무시하고 end_date 이전 전체를 대상으로 합니다."
        )
        start_date = None

    # 로깅
    date_filter_desc = format_date_filter_desc(start_date, end_date)
    logging.info(f"📋 [{hmi_id}] HMI RAW DATA 파일 목록 조회 시작: {remote_base_path}")
    logging.info(f"   대상 파일 패턴: {REMOTE_FILE_PATTERNS}")
    logging.info(f"   날짜 필터: {date_filter_desc}")
    
    # SFTP 연결 및 파일 목록 조회
    sftp_hook = SFTPHook(ftp_conn_id=sftp_conn_id)
    try:
        with sftp_hook.get_conn() as sftp:
            sftp.chdir(remote_base_path)
            logging.info(f"[{hmi_id}] 현재 원격 디렉토리: {sftp.getcwd()}")
            
            all_items = sftp.listdir('.')
            logging.info(f"전체 항목 수: {len(all_items)}")
            
            remote_files = filter_remote_files(sftp, all_items, remote_base_path, start_date, end_date)
            
            total_size = sum(f["size"] for f in remote_files)
            logging.info(f"✅ [{hmi_id}] 처리 대상 파일 수: {len(remote_files)}개")
            logging.info(f"   [{hmi_id}] 총 용량: {total_size:,} bytes ({total_size / 1024 / 1024:.2f} MB)")
            
            # 날짜 형식 결정 (hourly인 경우 시간까지 포함)
            if is_hourly:
                start_date_str = start_date.strftime('%Y-%m-%d %H:%M') if start_date else None
                end_date_str = end_date.strftime('%Y-%m-%d %H:%M') if end_date else None
            else:
                start_date_str = start_date.strftime('%Y-%m-%d') if start_date else None
                end_date_str = end_date.strftime('%Y-%m-%d') if end_date else None
            
            result = {
                "status": "success",
                "hmi_id": hmi_id,
                "remote_path": remote_base_path,
                "files": remote_files,
                "total_count": len(remote_files),
                "total_size_bytes": total_size,
                "file_patterns": REMOTE_FILE_PATTERNS,
                "start_date": start_date_str,
                "end_date": end_date_str
            }
        
        # with 블록 종료 후 명시적으로 연결 종료
        try:
            sftp_hook.close_conn()
            logging.debug(f"🔌 [{hmi_id}] SFTP 연결 명시적으로 종료")
        except Exception as close_err:
            logging.warning(f"⚠️ [{hmi_id}] SFTP 연결 종료 중 경고: {close_err}")
        
        return result
    except Exception as e:
        # 오류 발생 시에도 연결 종료 시도
        try:
            if sftp_hook:
                sftp_hook.close_conn()
                logging.debug(f"🔌 [{hmi_id}] 오류 발생 후 SFTP 연결 종료")
        except Exception as close_err:
            logging.warning(f"⚠️ [{hmi_id}] 오류 발생 후 SFTP 연결 종료 중 경고: {close_err}")
        
        # 연결 실패인 경우 Skip 처리
        error_str = str(e)
        error_lower = error_str.lower()
        is_connection_error = (
            "connection" in error_lower or
            "timeout" in error_lower or
            "timed out" in error_lower or
            "connection reset" in error_lower or
            "reset by peer" in error_lower or
            "errno 104" in error_lower or
            "banner" in error_lower or
            "protocol" in error_lower or
            "network" in error_lower
        )
        is_auth_error = "authentication" in error_lower or "password" in error_lower
        
        if is_connection_error and not is_auth_error:
            # Skip 전에 end_date 정보를 XCom에 저장 (Variable 업데이트용)
            try:
                ti = kwargs.get('ti')
                if ti and end_date:
                    # 날짜 형식 결정 (hourly인 경우 시간까지 포함)
                    if is_hourly:
                        end_date_str = end_date.strftime('%Y-%m-%d %H:%M') if end_date else None
                    else:
                        end_date_str = end_date.strftime('%Y-%m-%d') if end_date else None
                    
                    if end_date_str:
                        skip_result = {
                            "status": "skipped",
                            "hmi_id": hmi_id,
                            "end_date": end_date_str,
                            "reason": "connection_failed"
                        }
                        ti.xcom_push(key='skip_result', value=skip_result)
                        logging.info(f"ℹ️ [{hmi_id}] Skip 전 end_date XCom 저장: {end_date_str}")
            except Exception as xcom_err:
                logging.warning(f"⚠️ [{hmi_id}] XCom 저장 중 오류 (무시): {xcom_err}")
            
            skip_msg = (
                f"⏭️ [{hmi_id}] 파일 목록 조회 중 SFTP 연결 불가 - 태스크 Skip\n"
                f"원인: {error_str}\n"
                f"설명: 장비가 전원이 꺼져있거나 네트워크 연결이 불가능합니다.\n"
                f"      다음 실행 시 자동으로 재시도됩니다."
            )
            logging.warning(skip_msg)
            raise AirflowSkipException(skip_msg) from e
        
        # 그 외 오류는 그대로 raise
        logging.error(f"❌ [{hmi_id}] 파일 목록 조회 실패: {e}")
        raise


def download_files(hmi_config: dict, **kwargs) -> dict:
    """원격 파일 다운로드 (HMI별)"""
    ti = kwargs['ti']
    hmi_id = hmi_config['hmi_id']
    sftp_conn_id = hmi_config['sftp_conn_id']
    remote_base_path = normalize_remote_path(hmi_config['remote_base_path'])
    local_save_path = hmi_config['local_save_path']
    
    # HMI별 task_id로 XCom에서 데이터 가져오기
    list_task_id = f"list_remote_files_{hmi_id}"
    list_result = ti.xcom_pull(task_ids=list_task_id)
    
    if not list_result or list_result.get('status') != 'success':
        raise ValueError(f"[{hmi_id}] 파일 목록 조회 실패")
    
    remote_files = list_result.get('files', [])
    if not remote_files:
        logging.warning(f"⚠️ [{hmi_id}] 다운로드할 파일이 없습니다")
        return {
            "status": "success",
            "hmi_id": hmi_id,
            "downloaded_files": [],
            "total_count": 0
        }
    
    logging.info(f"📥 [{hmi_id}] 파일 다운로드 시작: {len(remote_files)}개 파일")
    logging.info(f"   로컬 저장 경로: {local_save_path}")
    
    # 총 다운로드 용량 계산 및 디스크 공간 확인
    total_size_bytes = sum(f.get('size', 0) for f in remote_files)
    total_size_gb = total_size_bytes / 1024 / 1024 / 1024
    logging.info(f"   총 다운로드 용량: {total_size_gb:.2f} GB ({total_size_bytes:,} bytes)")
    
    # 디스크 공간 확인
    if not check_disk_space(local_save_path, total_size_bytes):
        raise ValueError(
            f"[{hmi_id}] 디스크 공간이 부족합니다. "
            f"필요: {total_size_gb:.2f} GB"
        )
    
    sftp_hook = SFTPHook(ftp_conn_id=sftp_conn_id)
    downloaded_files = []
    failed_files = []
    
    try:
        with sftp_hook.get_conn() as sftp:
            # 원격 디렉토리로 이동 (list_remote_files와 동일한 방식)
            sftp.chdir(remote_base_path)
            logging.info(f"[{hmi_id}] 원격 디렉토리 변경: {sftp.getcwd()}")
            
            for file_info in remote_files:
                remote_path = file_info['path']
                file_name = file_info['name']
                
                # 로컬 저장 경로에 파일명만 저장
                local_path = Path(local_save_path) / file_name
                local_dir = local_path.parent
                
                # 로컬 디렉토리 생성
                ensure_local_directory(str(local_dir))
                
                try:
                    # 파일 크기 (0일 수 있음 - Windows SFTP 조회 실패)
                    file_size_bytes = file_info.get('size', 0)
                    file_size_mb = file_size_bytes / 1024 / 1024 if file_size_bytes > 0 else 0
                    
                    # 대용량 파일인 경우 상세 정보 로깅
                    if file_size_mb >= LARGE_FILE_THRESHOLD_MB:
                        logging.info(
                            f"다운로드 중 (대용량 파일): {remote_path} -> {local_path} "
                            f"({file_size_mb:.2f} MB, 예상 시간: {file_size_mb * 0.1:.1f}초)"
                        )
                    elif file_size_mb > 0:
                        logging.info(f"다운로드 중: {remote_path} -> {local_path} ({file_size_mb:.2f} MB)")
                    else:
                        logging.info(f"다운로드 중: {remote_path} -> {local_path} (크기 확인 중...)")
                    
                    # 파일 다운로드 (현재 디렉토리에 있으므로 파일명만 사용)
                    start_time = time.time()
                    sftp.get(file_name, str(local_path))
                    elapsed_time = time.time() - start_time
                    
                    # 다운로드 속도 계산 및 로깅
                    local_size_mb = local_path.stat().st_size / 1024 / 1024
                    if elapsed_time > 0 and local_size_mb > 0:
                        download_speed_mbps = local_size_mb / elapsed_time
                        logging.info(
                            f"  ⏱️ 다운로드 시간: {elapsed_time:.1f}초, "
                            f"속도: {download_speed_mbps:.2f} MB/s"
                        )
                    
                    # 파일 크기 검증
                    if VERIFY_FILE_SIZE:
                        local_size = local_path.stat().st_size
                        remote_size = file_info.get('size', 0)
                        
                        # 원격 파일 크기가 0인 경우 재조회
                        if remote_size == 0:
                            try:
                                sftp.chdir(remote_base_path)
                                remote_size = sftp.stat(file_name).st_size
                                logging.debug(f"  원격 파일 크기 재조회: {file_name} = {remote_size:,} bytes")
                            except Exception as e:
                                logging.warning(f"  원격 파일 크기 재조회 실패: {file_name} - {e}")
                                logging.info(f"  ⚠️ 원격 크기 확인 불가, 로컬 크기만 확인: {local_size:,} bytes")
                                remote_size = local_size
                        
                        if local_size != remote_size:
                            raise ValueError(
                                f"파일 크기 불일치: 로컬={local_size}, 원격={remote_size}"
                            )
                    
                    downloaded_files.append({
                        "name": file_name,
                        "remote_path": remote_path,
                        "local_path": str(local_path),
                        "size": local_path.stat().st_size
                    })
                    logging.info(f"  ✅ [{hmi_id}] 다운로드 완료: {file_name} ({local_path.stat().st_size:,} bytes)")
                    
                except Exception as e:
                    logging.error(f"  ❌ [{hmi_id}] 다운로드 실패: {file_name} - {e}")
                    failed_files.append({
                        "name": file_name,
                        "remote_path": remote_path,
                        "error": str(e)
                    })
        
        # with 블록 종료 후 명시적으로 연결 종료
        try:
            sftp_hook.close_conn()
            logging.debug(f"🔌 [{hmi_id}] SFTP 연결 명시적으로 종료")
        except Exception as close_err:
            logging.warning(f"⚠️ [{hmi_id}] SFTP 연결 종료 중 경고: {close_err}")
        
        logging.info(f"✅ [{hmi_id}] 다운로드 완료: 성공={len(downloaded_files)}, 실패={len(failed_files)}")
        
        return {
            "status": "success",
            "hmi_id": hmi_id,
            "downloaded_files": downloaded_files,
            "failed_files": failed_files,
            "total_count": len(downloaded_files)
        }
    except Exception as e:
        # 오류 발생 시에도 연결 종료 시도
        try:
            if sftp_hook:
                sftp_hook.close_conn()
                logging.debug(f"🔌 [{hmi_id}] 오류 발생 후 SFTP 연결 종료")
        except Exception as close_err:
            logging.warning(f"⚠️ [{hmi_id}] 오류 발생 후 SFTP 연결 종료 중 경고: {close_err}")
        
        # 연결 실패인 경우 Skip 처리
        error_str = str(e)
        error_lower = error_str.lower()
        is_connection_error = (
            "connection" in error_lower or
            "timeout" in error_lower or
            "timed out" in error_lower or
            "connection reset" in error_lower or
            "reset by peer" in error_lower or
            "errno 104" in error_lower or
            "banner" in error_lower or
            "protocol" in error_lower or
            "network" in error_lower
        )
        is_auth_error = "authentication" in error_lower or "password" in error_lower
        
        if is_connection_error and not is_auth_error:
            skip_msg = (
                f"⏭️ [{hmi_id}] 파일 다운로드 중 SFTP 연결 불가 - 태스크 Skip\n"
                f"원인: {error_str}\n"
                f"설명: 장비가 전원이 꺼져있거나 네트워크 연결이 불가능합니다.\n"
                f"      다음 실행 시 자동으로 재시도됩니다."
            )
            logging.warning(skip_msg)
            raise AirflowSkipException(skip_msg) from e
        
        # 그 외 오류는 그대로 raise
        logging.error(f"❌ [{hmi_id}] 다운로드 중 오류: {e}")
        raise


def generate_summary_report(**kwargs) -> dict:
    """모든 HMI 작업 완료 요약 보고서 생성"""
    ti = kwargs['ti']
    
    all_results = []
    total_files_listed = 0
    total_files_downloaded = 0
    total_files_deleted_remote = 0
    total_size_listed = 0
    total_size_downloaded = 0
    
    # prepare_date_range에서 설정한 공통 end_date 가져오기 (Skip된 HMI를 위한 fallback)
    date_range = ti.xcom_pull(task_ids='prepare_date_range')
    common_end_date = None
    if date_range and date_range.get('end_date'):
        common_end_date = date_range.get('end_date')
    
    # 각 HMI별 결과 수집
    for hmi_config in HMI_CONFIGS:
        hmi_id = hmi_config['hmi_id']
        list_task_id = f"list_remote_files_{hmi_id}"
        download_task_id = f"download_files_{hmi_id}"
        cleanup_task_id = f"cleanup_old_files_{hmi_id}"
        
        list_result = ti.xcom_pull(task_ids=list_task_id)
        download_result = ti.xcom_pull(task_ids=download_task_id)
        # Incremental/Hourly에서만 존재하는 cleanup 태스크 결과 (Backfill에서는 None)
        cleanup_result = ti.xcom_pull(task_ids=cleanup_task_id)
        
        # 각 HMI별 end_date 저장 (Variable 업데이트용)
        # Skip된 경우 공통 end_date 사용
        hmi_end_date = None
        if list_result:
            hmi_end_date = list_result.get('end_date')
        elif common_end_date:
            # Skip된 경우 공통 end_date 사용
            hmi_end_date = common_end_date
        
        # 원격 삭제 통계
        deleted_count = 0
        if cleanup_result and isinstance(cleanup_result, dict):
            deleted = cleanup_result.get("deleted") or []
            deleted_count = len(deleted)
            total_files_deleted_remote += deleted_count
        
        all_results.append({
            "hmi_id": hmi_id,
            "list_result": list_result,
            "download_result": download_result,
            "cleanup_result": cleanup_result,
            "deleted_count": deleted_count,
            "end_date": hmi_end_date,
        })
        
        # 통계 집계
        if list_result:
            total_files_listed += list_result.get('total_count', 0)
            total_size_listed += list_result.get('total_size_bytes', 0)
        
        if download_result:
            total_files_downloaded += download_result.get('total_count', 0)
            downloaded_files = download_result.get('downloaded_files', [])
            total_size_downloaded += sum(f.get('size', 0) for f in downloaded_files)
    
    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_hmi_count": len(HMI_CONFIGS),
        "results": all_results,
        "totals": {
            "files_listed": total_files_listed,
            "files_downloaded": total_files_downloaded,
            "files_deleted_remote": total_files_deleted_remote,
            "size_listed_bytes": total_size_listed,
            "size_downloaded_bytes": total_size_downloaded,
        }
    }
    
    logging.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logging.info("📊 HMI RAW DATA 수집 작업 완료 요약 (전체)")
    logging.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logging.info(f"  📋 처리된 HMI 수: {len(HMI_CONFIGS)}개")
    logging.info(f"  📋 조회된 파일: {total_files_listed}개 ({total_size_listed / 1024 / 1024:.2f} MB)")
    logging.info(f"  📥 다운로드 완료: {total_files_downloaded}개 ({total_size_downloaded / 1024 / 1024:.2f} MB)")
    logging.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    # HMI별 상세 정보
    skipped_count = 0
    for result in all_results:
        hmi_id = result['hmi_id']
        list_result = result['list_result']
        download_result = result['download_result']
        deleted_count = result.get('deleted_count', 0)
        
        logging.info(f"  [{hmi_id}]")
        if list_result:
            logging.info(f"    - 조회: {list_result.get('total_count', 0)}개")
        elif download_result is None:
            logging.info(f"    - 상태: ⏭️ Skip됨 (SFTP 연결 불가)")
            skipped_count += 1
        if download_result:
            logging.info(f"    - 다운로드: {download_result.get('total_count', 0)}개")
        if deleted_count:
            logging.info(f"    - 원격 삭제: {deleted_count}개")
    
    if skipped_count > 0:
        logging.info(f"  ⏭️ Skip된 HMI: {skipped_count}개 (다음 실행 시 재시도)")
    
    logging.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    return summary


def update_variable_after_run(**kwargs):
    """처리 완료 후 각 HMI별 Variable 업데이트 (Incremental/Hourly용)"""
    ti = kwargs['ti']
    dag = kwargs.get('dag')
    summary = ti.xcom_pull(task_ids='generate_summary_report')
    
    if not summary:
        logging.warning(f"⚠️ Variable 업데이트 실패: summary가 없습니다.")
        return
    
    # DAG ID 확인하여 hourly 여부 판단 (incremental도 매시간 실행하므로 hourly로 처리)
    is_hourly = dag and (dag.dag_id.endswith('_hourly') or dag.dag_id.endswith('_incremental')) if dag else False
    
    # prepare_date_range에서 설정한 공통 end_date 가져오기 (Skip된 HMI를 위한 fallback)
    date_range = ti.xcom_pull(task_ids='prepare_date_range')
    common_end_date = None
    if date_range and date_range.get('end_date'):
        common_end_date = date_range.get('end_date')
    
    # 각 HMI별로 Variable 업데이트
    updated_count = 0
    skipped_count = 0
    
    for result in summary.get('results', []):
        hmi_id = result.get('hmi_id')
        end_date = result.get('end_date')
        
        # end_date가 없는 경우 (Skip된 경우) 공통 end_date 사용
        if not end_date and common_end_date:
            end_date = common_end_date
            logging.info(f"ℹ️ [{hmi_id}] Skip된 HMI - 공통 end_date 사용: {end_date}")
        
        if hmi_id and end_date:
            # HMI_CONFIGS에서 해당 hmi_id의 설정 찾기
            hmi_config = next((config for config in HMI_CONFIGS if config.get('hmi_id') == hmi_id), None)
            if not hmi_config:
                logging.warning(f"⚠️ [{hmi_id}] HMI 설정을 찾을 수 없습니다. Variable 업데이트 건너뜀.")
                continue
            
            variable_key = get_variable_key(hmi_config)
            
            # Skip 여부 확인
            list_result = result.get('list_result')
            download_result = result.get('download_result')
            was_skipped = (list_result is None and download_result is None) or \
                         (list_result is None)  # list_remote_files가 Skip된 경우
            
            # Hourly인 경우 시간까지 저장, Daily인 경우 날짜만 저장
            # end_date는 이미 문자열로 저장되어 있음 (prepare_date_range에서 설정)
            Variable.set(variable_key, end_date)
            
            if was_skipped:
                skipped_count += 1
                if is_hourly:
                    logging.info(f"✅ [{hmi_id}] Variable '{variable_key}' 업데이트 (Skip된 경우, Hourly): {end_date}")
                else:
                    logging.info(f"✅ [{hmi_id}] Variable '{variable_key}' 업데이트 (Skip된 경우, Daily): {end_date}")
            else:
                if is_hourly:
                    logging.info(f"✅ [{hmi_id}] Variable '{variable_key}' 업데이트 (Hourly): {end_date}")
                else:
                    logging.info(f"✅ [{hmi_id}] Variable '{variable_key}' 업데이트 (Daily): {end_date}")
            updated_count += 1
        else:
            logging.warning(f"⚠️ [{hmi_id}] Variable 업데이트 실패: end_date가 없습니다. (공통 end_date도 없음)")
    
    logging.info(f"✅ 총 {updated_count}개 HMI의 Variable 업데이트 완료 (Skip된 HMI: {skipped_count}개)")



def cleanup_old_remote_files(hmi_config: dict, **kwargs) -> dict:
    """원격 SFTP 경로(remote_base_path)에서 retention_days 이전 파일 삭제 (HMI별)"""
    from pipeline.data_transfer.hmi_raw_file_etl_config import INDO_TZ

    hmi_id = hmi_config["hmi_id"]
    sftp_conn_id = hmi_config["sftp_conn_id"]
    remote_base_path = normalize_remote_path(hmi_config["remote_base_path"])

    retention_days = int(
        hmi_config.get("remote_retention_days", hmi_config.get("local_retention_days", 30))
    )
    cutoff = datetime.now(INDO_TZ) - timedelta(days=retention_days)

    if not hmi_config.get("remote_cleanup_enabled", True):
        logging.info(
            f"ℹ️ [{hmi_id}] cleanup_old_remote_files: remote_cleanup_enabled=False, 원격 삭제 건너뜀."
        )
        return {
            "status": "skipped",
            "hmi_id": hmi_id,
            "reason": "disabled",
            "deleted": [],
            "skipped": [],
        }

    logging.info(
        f"🧹 [{hmi_id}] cleanup_old_remote_files 시작 - 원격 경로: {remote_base_path}, "
        f"보존 기간: {retention_days}일 (cutoff={cutoff})"
    )

    sftp_hook = SFTPHook(ftp_conn_id=sftp_conn_id)
    deleted = []
    skipped = []

    try:
        with sftp_hook.get_conn() as sftp:
            sftp.chdir(remote_base_path)
            logging.info(f"[{hmi_id}] 원격 정리 대상 디렉토리: {sftp.getcwd()}")

            # listdir_attr 로 mtime 포함된 정보 가져오기
            entries = sftp.listdir_attr(".")
            logging.info(f"[{hmi_id}] 원격 항목 수: {len(entries)}")

            for entry in entries:
                name = entry.filename

                # 파일 패턴 필터 (*.csv 등)
                if not any(fnmatch.fnmatch(name, pattern) for pattern in REMOTE_FILE_PATTERNS):
                    continue

                try:
                    mtime = datetime.fromtimestamp(entry.st_mtime, tz=INDO_TZ)
                except Exception as e:
                    logging.warning(
                        f"⚠️ [{hmi_id}] 원격 파일 mtime 확인 실패: {name} - {e}"
                    )
                    skipped.append(
                        {"name": name, "reason": "stat_failed", "error": str(e)}
                    )
                    continue

                if mtime < cutoff:
                    try:
                        sftp.remove(name)
                        logging.info(
                            f"  🗑️ [{hmi_id}] 원격 오래된 파일 삭제: {name} (mtime={mtime})"
                        )
                        deleted.append({"name": name, "mtime": mtime.isoformat()})
                    except Exception as e:
                        logging.error(
                            f"  ❌ [{hmi_id}] 원격 파일 삭제 실패: {name} - {e}"
                        )
                        skipped.append(
                            {
                                "name": name,
                                "reason": "delete_failed",
                                "error": str(e),
                            }
                        )
                else:
                    skipped.append(
                        {
                            "name": name,
                            "reason": "newer_than_cutoff",
                            "mtime": mtime.isoformat(),
                        }
                    )

        try:
            sftp_hook.close_conn()
        except Exception:
            pass

        logging.info(
            f"🧹 [{hmi_id}] cleanup_old_remote_files 완료 - 삭제 성공={len(deleted)}개, "
            f"유지/실패 포함 기타={len(skipped)}개"
        )

        return {
            "status": "success",
            "hmi_id": hmi_id,
            "deleted": deleted,
            "skipped": skipped,
        }
    except Exception as e:
        # 오류 발생 시에도 연결 종료 시도
        try:
            if sftp_hook:
                sftp_hook.close_conn()
        except Exception:
            pass

        logging.error(f"❌ [{hmi_id}] cleanup_old_remote_files 중 오류: {e}")
        raise
