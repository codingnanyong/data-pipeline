"""공통 함수 모듈 - IPI Temperature Matching"""
import logging
import os
import time
import requests
from urllib.parse import quote, urlparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Tuple, List, Dict, Optional
from plugins.hooks.postgres_hook import PostgresHelper


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

QUALITY_POSTGRES_CONN_ID = "pg_jj_quality_dw"
LIVY_URL = "http://10.10.100.80:30998"
# Livy batch의 `file` — HDFS에 올린 스크립트 경로 (워크스페이스 원본은 아래와 동일 파일을 업로드)
#   소스: hadoop/k8s/spark/jobs/ipi_temperature_matching_spark.py
#   배포 예: hdfs dfs -put -f ... hdfs:///jobs/ipi_temperature_matching_spark.py
# 클러스터 측: Livy(hadoop/k8s/livy)가 YARN에 spark-submit → Spark History / YARN RM 과 연동.
# 수동 검증용 Pod: hadoop/k8s/spark/spark-client.yaml (spark-submit·디버깅, Airflow 경로와 별개)
LIVY_JOB_PATH = "hdfs:///jobs/ipi_temperature_matching_spark.py"
LIVY_JDBC_JAR = "hdfs:///jars/postgresql-jdbc.jar"

# hadoop/k8s 기준 NodePort (docs/08-livy.md, resourcemanager-service, spark-history, namenode-service)
# Airflow 워커가 다른 네트워크면 SPARK_STACK_NODE_HOST 또는 각 *_WEB_URL 환경변수로 덮어쓰기.


def _spark_stack_node_host() -> str:
    return os.environ.get("SPARK_STACK_NODE_HOST") or (urlparse(LIVY_URL).hostname or "127.0.0.1")


def _yarn_rm_web_url() -> str:
    return os.environ.get("SPARK_YARN_RM_WEB_URL") or f"http://{_spark_stack_node_host()}:30890"


def _spark_history_web_url() -> str:
    return os.environ.get("SPARK_HISTORY_WEB_URL") or f"http://{_spark_stack_node_host()}:30180"


def _hdfs_namenode_web_url() -> str:
    return os.environ.get("HDFS_NAMENODE_WEB_URL") or f"http://{_spark_stack_node_host()}:30870"


def _hdfs_uri_to_absolute_path(hdfs_uri: str) -> str:
    """hdfs:///jobs/a.py → /jobs/a.py (WebHDFS 경로)."""
    p = urlparse(hdfs_uri)
    if p.scheme != "hdfs":
        raise ValueError(f"hdfs URI 가 아님: {hdfs_uri}")
    path = p.path or "/"
    return path if path.startswith("/") else f"/{path}"


def verify_hdfs_livy_artifacts_exist(nn_base: str) -> Dict[str, str]:
    """WebHDFS GETFILESTATUS 로 Livy batch에 넣는 스크립트·JAR 존재 확인. 없으면 예외.

    Airflow 워커가 NameNode WebHDFS에 닿지 않으면 여기서 실패한다(잡 제출 전에 원인 노출).
    긴급 시에만 환경변수 SPARK_SKIP_HDFS_ARTIFACT_CHECK=1 로 검사 생략 가능.
    """
    if os.environ.get("SPARK_SKIP_HDFS_ARTIFACT_CHECK", "").strip().lower() in ("1", "true", "yes"):
        logging.warning(
            "  [HDFS] SPARK_SKIP_HDFS_ARTIFACT_CHECK=1 — 스크립트/JAR 파일 존재 검사 생략 (비권장)"
        )
        return {"hdfs_job_py": "skipped", "hdfs_jdbc_jar": "skipped"}

    out: Dict[str, str] = {}
    base = nn_base.rstrip("/")
    for label, uri, hint in (
        (
            "hdfs_job_py",
            LIVY_JOB_PATH,
            "hdfs dfs -put -f hadoop/k8s/spark/jobs/ipi_temperature_matching_spark.py hdfs:///jobs/",
        ),
        (
            "hdfs_jdbc_jar",
            LIVY_JDBC_JAR,
            "예: hdfs dfs -put -f postgresql-*.jar hdfs:///jars/postgresql-jdbc.jar",
        ),
    ):
        abs_path = _hdfs_uri_to_absolute_path(uri)
        quoted = quote(abs_path, safe="/")
        url = f"{base}/webhdfs/v1{quoted}"
        try:
            r = requests.get(url, params={"op": "GETFILESTATUS"}, timeout=20)
        except requests.RequestException as exc:
            raise RuntimeError(
                f"HDFS WebHDFS 접속 불가 (Airflow 워커→NameNode {base}): {exc}\n"
                f"방화벽/NodePort(30870) 확인. 또는 SPARK_SKIP_HDFS_ARTIFACT_CHECK=1 (임시)"
            ) from exc

        if r.status_code == 404:
            raise RuntimeError(
                f"HDFS에 파일이 없습니다: {uri} (WebHDFS path={abs_path})\n"
                f"배포: {hint}"
            )
        if not r.ok:
            raise RuntimeError(
                f"HDFS GETFILESTATUS 실패 HTTP {r.status_code} path={abs_path} body={r.text[:800]}"
            )

        try:
            data = r.json()
        except ValueError as exc:
            raise RuntimeError(f"HDFS 응답 JSON 아님: {r.text[:500]}") from exc

        fs = data.get("FileStatus") if isinstance(data, dict) else None
        if not fs:
            raise RuntimeError(f"HDFS FileStatus 없음: {data}")
        ftype = fs.get("type")
        flen = fs.get("length")
        if ftype != "FILE":
            raise RuntimeError(
                f"HDFS 경로가 파일이 아님 (type={ftype}): {uri}. 디렉터리면 경로를 확인하세요."
            )
        logging.info(
            "  [HDFS] OK %s → %s (type=%s, length=%s)",
            label,
            abs_path,
            ftype,
            flen,
        )
        out[label] = f"ok:{flen}"

    return out


GOOD_PRODUCT_SCHEMA = "silver"
GOOD_PRODUCT_TABLE = "ipi_good_product"
OSND_CROSS_VALIDATED_SCHEMA = "silver"
OSND_CROSS_VALIDATED_TABLE = "ipi_defective_cross_validated"
TEMPERATURE_SCHEMA = "silver"
TEMPERATURE_TABLE = "ipi_anomaly_transformer_result"
DEFECT_CODE_SCHEMA = "silver"
DEFECT_CODE_TABLE = "ip_defect_code"
TARGET_SCHEMA = "gold"
TARGET_TABLE = "ipi_temperature_matching"
TARGET_DETAIL_TABLE = "ipi_temperature_matching_detail"
TEMPERATURE_LOOKBACK_MINUTES = 7
ALLOWED_REASON_CDS = ['good', 'Burning', 'Sink mark']

# Machine Configuration
# 처리할 machine_no 리스트 (ipi_anomaly_transformer_common.py와 동일하게 설정)
# 예: MACHINE_NO_LIST = ["MCA34", "MCA20", "MCA37"]  # 여러 개 처리
MACHINE_NO_LIST = ["MCA34"]  # 현재 MCA34만 처리 (온도 데이터와 일치시켜야 함)


# ════════════════════════════════════════════════════════════════
# 2️⃣ Data Extraction
# ════════════════════════════════════════════════════════════════

def extract_good_product_data(pg: PostgresHelper, start_date: str, end_date: str) -> pd.DataFrame:
    """양품 데이터 추출 (머신 필터링 적용)"""
    logging.info(f"1️⃣ 양품 데이터 추출: {start_date} ~ {end_date}")
    
    # 머신 필터링 조건 추가
    if MACHINE_NO_LIST and len(MACHINE_NO_LIST) > 0:
        machine_list_str = ','.join([f"'{mno}'" for mno in MACHINE_NO_LIST])
        machine_filter = f"AND mc_cd IN ({machine_list_str})"
        logging.info(f"🏭 머신 필터링 적용: {', '.join(MACHINE_NO_LIST)}")
    else:
        machine_filter = ""
        logging.warning("⚠️ 머신 필터링이 설정되지 않았습니다. 모든 머신 데이터를 사용합니다.")
    
    sql = f"""SELECT so_id, rst_ymd, mc_cd, st_num, st_side, mold_id
              FROM {GOOD_PRODUCT_SCHEMA}.{GOOD_PRODUCT_TABLE}
              WHERE DATE(rst_ymd) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
              {machine_filter}"""
    data = pg.execute_query(sql, task_id="extract_good_product", xcom_key=None)
    df = pd.DataFrame(data, columns=['so_id', 'rst_ymd', 'mc_cd', 'st_num', 'st_side', 'mold_id']) if data else pd.DataFrame(columns=['so_id', 'rst_ymd', 'mc_cd', 'st_num', 'st_side', 'mold_id'])
    df.columns = df.columns.str.lower()
    logging.info(f"✅ 양품 데이터 추출 완료: {len(df):,} rows")
    return df


def extract_osnd_data(pg: PostgresHelper, start_date: str, end_date: str) -> pd.DataFrame:
    """OSND 데이터 추출 (머신 필터링 적용)"""
    logging.info(f"2️⃣ OSND 데이터 추출: {start_date} ~ {end_date}")
    col_sql = f"""SELECT column_name FROM information_schema.columns
                 WHERE table_schema = '{OSND_CROSS_VALIDATED_SCHEMA}' AND table_name = '{OSND_CROSS_VALIDATED_TABLE}'
                 ORDER BY ordinal_position"""
    columns = pg.execute_query(col_sql, task_id="get_osnd_columns", xcom_key=None)
    column_names = [col[0].lower() for col in columns] if columns else []
    
    # 머신 필터링 조건 추가
    if MACHINE_NO_LIST and len(MACHINE_NO_LIST) > 0:
        machine_list_str = ','.join([f"'{mno}'" for mno in MACHINE_NO_LIST])
        machine_filter = f"AND machine_cd IN ({machine_list_str})"
        logging.info(f"🏭 머신 필터링 적용: {', '.join(MACHINE_NO_LIST)}")
    else:
        machine_filter = ""
        logging.warning("⚠️ 머신 필터링이 설정되지 않았습니다. 모든 머신 데이터를 사용합니다.")
    
    sql = f"""SELECT * FROM {OSND_CROSS_VALIDATED_SCHEMA}.{OSND_CROSS_VALIDATED_TABLE}
              WHERE DATE(osnd_dt) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
              {machine_filter}"""
    data = pg.execute_query(sql, task_id="extract_osnd_data", xcom_key=None)
    df = pd.DataFrame(data, columns=column_names) if data else pd.DataFrame(columns=column_names)
    logging.info(f"✅ OSND 데이터 추출 완료: {len(df):,} rows")
    return df


def extract_temperature_data(pg: PostgresHelper, start_date: str, end_date: str) -> pd.DataFrame:
    """온도 데이터 추출 (시간 범위 및 machine_code 기준)"""
    logging.info(f"3️⃣ 온도 데이터 추출: {start_date} ~ {end_date}")
    start_dt = datetime.strptime(start_date, '%Y-%m-%d') - timedelta(minutes=TEMPERATURE_LOOKBACK_MINUTES)
    end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)
    
    # machine_code 필터링 조건 추가
    if MACHINE_NO_LIST and len(MACHINE_NO_LIST) > 0:
        machine_list_str = ','.join([f"'{mno}'" for mno in MACHINE_NO_LIST])
        machine_filter = f"AND machine_code IN ({machine_list_str})"
        logging.info(f"🏭 머신 필터링 적용: {', '.join(MACHINE_NO_LIST)}")
    else:
        machine_filter = ""
        logging.warning("⚠️ 머신 필터링이 설정되지 않았습니다. 모든 머신 데이터를 사용합니다.")
    
    # 시간 범위 및 machine_code로 필터링
    sql = f"""SELECT machine_code, mc, prop, measurement_time, temperature
              FROM {TEMPERATURE_SCHEMA}.{TEMPERATURE_TABLE}
              WHERE measurement_time BETWEEN TIMESTAMP '{start_dt}' AND TIMESTAMP '{end_dt}'
              {machine_filter}"""
    data = pg.execute_query(sql, task_id="extract_temperature", xcom_key=None)
    df = pd.DataFrame(data, columns=['machine_code', 'mc', 'prop', 'measurement_time', 'temperature']) if data else pd.DataFrame(columns=['machine_code', 'mc', 'prop', 'measurement_time', 'temperature'])
    df.columns = df.columns.str.lower()
    logging.info(f"✅ 온도 데이터 추출 완료: {len(df):,} rows")
    return df


def extract_defect_code_data(pg: PostgresHelper) -> pd.DataFrame:
    """불량 코드 마스터 추출"""
    logging.info("4️⃣ 불량 코드 마스터 추출 중...")
    sql = f"SELECT defect_cd, defect_name FROM {DEFECT_CODE_SCHEMA}.{DEFECT_CODE_TABLE}"
    data = pg.execute_query(sql, task_id="extract_defect_code", xcom_key=None)
    df = pd.DataFrame(data, columns=['defect_cd', 'defect_name']) if data else pd.DataFrame(columns=['defect_cd', 'defect_name'])
    df.columns = df.columns.str.lower()
    logging.info(f"✅ 불량 코드 마스터 추출 완료: {len(df):,} rows")
    return df


# ════════════════════════════════════════════════════════════════
# 3️⃣ Data Transformation
# ════════════════════════════════════════════════════════════════

def transform_good_to_osnd_format(df_good: pd.DataFrame) -> pd.DataFrame:
    """양품 데이터를 OSND 스키마로 변환"""
    if len(df_good) == 0:
        return pd.DataFrame()
    osnd_cols = ["osnd_id", "osnd_dt", "machine_cd", "station", "station_rl", "mold_id", "reason_cd", "size_cd", "lr_cd", "osnd_bt_qty"]
    df = df_good.rename(columns={"so_id": "osnd_id", "rst_ymd": "osnd_dt", "mc_cd": "machine_cd", "st_num": "station", "st_side": "station_rl"}).copy()
    df["reason_cd"], df["size_cd"], df["lr_cd"], df["osnd_bt_qty"] = "good", None, None, None
    df = df[osnd_cols].copy()
    df["osnd_dt"], df["station"] = pd.to_datetime(df["osnd_dt"], errors="coerce"), df["station"].astype(str)
    logging.info(f"✅ 양품 데이터 변환 완료: {len(df):,} rows")
    return df


def merge_osnd_data(df_osnd: pd.DataFrame, df_good_as_osnd: pd.DataFrame) -> pd.DataFrame:
    """OSND 데이터와 양품 데이터 통합"""
    osnd_cols = ["osnd_id", "osnd_dt", "machine_cd", "station", "station_rl", "mold_id", "reason_cd", "size_cd", "lr_cd", "osnd_bt_qty"]
    if len(df_good_as_osnd) > 0:
        df_osnd_sel = df_osnd[osnd_cols].copy() if all(col in df_osnd.columns for col in osnd_cols) else pd.DataFrame(columns=osnd_cols)
        if len(df_osnd_sel) > 0:
            df_osnd_sel["osnd_dt"], df_osnd_sel["station"] = pd.to_datetime(df_osnd_sel["osnd_dt"], errors="coerce"), df_osnd_sel["station"].astype(str)
        df_result = pd.concat([df_osnd_sel, df_good_as_osnd], ignore_index=True)
    else:
        df_result = df_osnd[osnd_cols].copy() if all(col in df_osnd.columns for col in osnd_cols) else pd.DataFrame(columns=osnd_cols)
        if len(df_result) > 0:
            df_result["osnd_dt"], df_result["station"] = pd.to_datetime(df_result["osnd_dt"], errors="coerce"), df_result["station"].astype(str)
    df_result = df_result.sort_values("osnd_dt", ascending=True).reset_index(drop=True)
    df_result["station"] = df_result["station"].astype(int)
    logging.info(f"✅ OSND 데이터 통합 완료: {len(df_result):,} rows")
    return df_result


def map_defect_codes(df_osnd: pd.DataFrame, df_defect_code: pd.DataFrame) -> pd.DataFrame:
    """불량 코드 매핑"""
    if len(df_defect_code) == 0:
        return df_osnd
    defect_dict = df_defect_code.set_index('defect_cd')['defect_name'].to_dict()
    df_osnd['reason_cd'] = df_osnd['reason_cd'].map(defect_dict).fillna(df_osnd['reason_cd'])
    df_filtered = df_osnd[df_osnd['reason_cd'].isin(ALLOWED_REASON_CDS)].reset_index(drop=True)
    logging.info(f"✅ 불량 코드 매핑 완료: {len(df_filtered):,} rows")
    return df_filtered


def match_temperature_data(df_osnd: pd.DataFrame, df_temp: pd.DataFrame) -> List[Dict]:
    """온도 데이터 매칭"""
    if len(df_temp) == 0:
        return []
    df_temp['mc_prop'] = df_temp['mc'] + '_' + df_temp['prop']
    df_temp['measurement_time'] = pd.to_datetime(df_temp['measurement_time'], errors="coerce")
    df_osnd['osnd_dt'] = pd.to_datetime(df_osnd['osnd_dt'], errors="coerce")
    df_osnd['station'], df_osnd['station_rl'] = df_osnd['station'].astype(int), df_osnd['station_rl'].astype(str)
    df_valid = df_osnd[df_osnd['osnd_dt'].notna() & df_osnd['station'].notna() & df_osnd['station_rl'].notna()].copy()
    if len(df_valid) == 0:
        return []
    df_valid['time_start'] = df_valid['osnd_dt'] - pd.Timedelta(minutes=TEMPERATURE_LOOKBACK_MINUTES)
    # station_rl을 대문자로 변환하여 일관성 유지 (원래 코드와 동일하게)
    df_valid['station_rl_upper'] = df_valid['station_rl'].str.upper()
    df_valid['mc_prop_L'] = 'st_' + df_valid['station'].astype(str) + '_Plate Temperature L' + df_valid['station_rl_upper']
    df_valid['mc_prop_U'] = 'st_' + df_valid['station'].astype(str) + '_Plate Temperature U' + df_valid['station_rl_upper']
    temp_dict = {mc_prop: df_temp[df_temp['mc_prop'] == mc_prop][['measurement_time', 'temperature']].sort_values('measurement_time') 
                 for mc_prop in df_temp['mc_prop'].unique()}
    
    # 디버깅: 매칭 가능한 mc_prop 목록 로깅
    available_mc_props = set(temp_dict.keys())
    logging.info(f"📊 사용 가능한 온도 센서 mc_prop 개수: {len(available_mc_props)}")
    if len(available_mc_props) > 0:
        sample_props = list(available_mc_props)[:5]
        logging.info(f"📊 샘플 mc_prop: {sample_props}")
    
    combined_rows = []
    matched_count = 0
    unmatched_count = 0
    for idx, row in df_valid.iterrows():
        try:
            osnd_time, time_start = row['osnd_dt'], row['time_start']
            mc_prop_L = row['mc_prop_L']
            mc_prop_U = row['mc_prop_U']
            
            # temp_dict에서 직접 조회 (더 안전한 방식)
            temp_low_df = temp_dict.get(mc_prop_L, pd.DataFrame())
            temp_upper_df = temp_dict.get(mc_prop_U, pd.DataFrame())
            
            # 시간 범위 필터링
            if len(temp_low_df) > 0:
                temp_low_filtered = temp_low_df[
                    (temp_low_df['measurement_time'] >= time_start) & 
                    (temp_low_df['measurement_time'] <= osnd_time)
                ]
                temp_low_list = temp_low_filtered.to_dict('records')
            else:
                temp_low_list = []
                if idx < 10:  # 처음 10개만 로깅
                    logging.debug(f"⚠️ 매칭 실패 (L): {mc_prop_L} (station={row['station']}, station_rl={row['station_rl']})")
            
            if len(temp_upper_df) > 0:
                temp_upper_filtered = temp_upper_df[
                    (temp_upper_df['measurement_time'] >= time_start) & 
                    (temp_upper_df['measurement_time'] <= osnd_time)
                ]
                temp_upper_list = temp_upper_filtered.to_dict('records')
            else:
                temp_upper_list = []
                if idx < 10:  # 처음 10개만 로깅
                    logging.debug(f"⚠️ 매칭 실패 (U): {mc_prop_U} (station={row['station']}, station_rl={row['station_rl']})")
            
            if len(temp_low_list) > 0 or len(temp_upper_list) > 0:
                matched_count += 1
            else:
                unmatched_count += 1
            combined_row = row.drop(['time_start', 'mc_prop_L', 'mc_prop_U', 'station_rl_upper']).to_dict()
            combined_row['temp_L'], combined_row['temp_U'] = temp_low_list, temp_upper_list
            combined_rows.append(combined_row)
        except Exception as e:
            logging.warning(f"❗ Index {idx} 오류: {e}")
            unmatched_count += 1
    
    logging.info(f"✅ 온도 데이터 매칭 완료: {len(combined_rows):,} rows")
    logging.info(f"📊 매칭 성공: {matched_count:,} rows, 매칭 실패: {unmatched_count:,} rows")
    if unmatched_count > 0:
        match_ratio = matched_count / (matched_count + unmatched_count) * 100
        logging.warning(f"⚠️ 매칭 성공률: {match_ratio:.2f}% ({matched_count}/{matched_count + unmatched_count})")
    
    return combined_rows


# ════════════════════════════════════════════════════════════════
# 4️⃣ Data Loading
# ════════════════════════════════════════════════════════════════

def calculate_temperature_stats(temp_list: List[Dict]) -> Tuple[int, Optional[float], Optional[float], Optional[float]]:
    """온도 통계 계산"""
    if not temp_list:
        return 0, None, None, None
    temps = [float(item.get('temperature', 0)) for item in temp_list if item.get('temperature') is not None]
    return (len(temps), float(np.mean(temps)), float(np.min(temps)), float(np.max(temps))) if temps else (0, None, None, None)


def load_data(pg: PostgresHelper, combined_rows: List[Dict], extract_time: datetime) -> dict:
    """Gold 테이블에 데이터 적재"""
    if len(combined_rows) == 0:
        return {"main_rows": 0, "detail_rows": 0}
    main_data = [(
        row.get('osnd_id'), row.get('osnd_dt'), row.get('machine_cd'), row.get('station'), row.get('station_rl'),
        row.get('mold_id'), row.get('reason_cd'), row.get('size_cd'), row.get('lr_cd'), row.get('osnd_bt_qty'),
        *calculate_temperature_stats(row.get('temp_L', [])), *calculate_temperature_stats(row.get('temp_U', [])), extract_time
    ) for row in combined_rows]
    main_cols = ["osnd_id", "osnd_dt", "machine_cd", "station", "station_rl", "mold_id", "reason_cd", "size_cd", "lr_cd", "osnd_bt_qty",
                 "temp_L_count", "temp_L_avg", "temp_L_min", "temp_L_max", "temp_U_count", "temp_U_avg", "temp_U_min", "temp_U_max", "etl_extract_time"]
    logging.info(f"📦 메인 테이블 적재: {len(main_data):,} rows")
    pg.insert_data(TARGET_SCHEMA, TARGET_TABLE, main_data, main_cols, ["osnd_id", "osnd_dt"], chunk_size=1000)
    detail_data = []
    for row in combined_rows:
        for temp_list, temp_type in [(row.get('temp_L', []), 'L'), (row.get('temp_U', []), 'U')]:
            for seq_no, item in enumerate(sorted(temp_list, key=lambda x: x.get('measurement_time')), 1):
                detail_data.append((row.get('osnd_id'), row.get('osnd_dt'), temp_type, pd.to_datetime(item.get('measurement_time')), float(item.get('temperature', 0)), seq_no, extract_time))
    if detail_data:
        detail_cols = ["osnd_id", "osnd_dt", "temp_type", "measurement_time", "temperature", "seq_no", "etl_extract_time"]
        logging.info(f"📦 상세 테이블 적재: {len(detail_data):,} rows")
        pg.insert_data(TARGET_SCHEMA, TARGET_DETAIL_TABLE, detail_data, detail_cols, ["osnd_id", "osnd_dt", "temp_type", "measurement_time"], chunk_size=1000)
    return {"main_rows": len(main_data), "detail_rows": len(detail_data)}


# ════════════════════════════════════════════════════════════════
# 5️⃣ Main ETL Logic
# ════════════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════════════
# 6️⃣ Spark (Livy) 제출 - _spark suffix 테이블 병렬 검증용
# ════════════════════════════════════════════════════════════════

def ensure_spark_tables_exist(pg: PostgresHelper, suffix: str = "_spark") -> None:
    """_spark suffix 테이블이 없으면 원본 테이블 스키마로 생성"""
    spark_main   = f"{TARGET_SCHEMA}.{TARGET_TABLE}{suffix}"
    spark_detail = f"{TARGET_SCHEMA}.{TARGET_DETAIL_TABLE}{suffix}"
    pg.execute_update(
        f"CREATE TABLE IF NOT EXISTS {spark_main} "
        f"(LIKE {TARGET_SCHEMA}.{TARGET_TABLE} INCLUDING ALL)",
        task_id="create_spark_main_table",
    )
    pg.execute_update(
        f"CREATE TABLE IF NOT EXISTS {spark_detail} "
        f"(LIKE {TARGET_SCHEMA}.{TARGET_DETAIL_TABLE} INCLUDING ALL)",
        task_id="create_spark_detail_table",
    )
    logging.info(f"✅ _spark 테이블 준비 완료: {spark_main}, {spark_detail}")


def verify_spark_execution_stack() -> Dict[str, str]:
    """Livy·YARN RM·Spark History·HDFS NN·Postgres까지 점검 (잡 제출 전).

    - Livy /batches 실패 시: 예외 (잡 제출 불가).
    - 그 외(YARN UI, History, HDFS Web): Airflow 워커에서 NodePort 미노출 시 경고만 (Livy 경로는 별개).

    Returns:
        요약 문자열 dict (로그·XCom용).
    """
    from airflow.hooks.base import BaseHook

    summary: Dict[str, str] = {}
    logging.info(
        "══ Spark 실행 스택 점검 시작 (노드=%s, Livy=%s) ══",
        _spark_stack_node_host(),
        LIVY_URL,
    )

    # 1) Livy — 필수
    try:
        vr = requests.get(f"{LIVY_URL}/version", timeout=5)
        if vr.ok:
            logging.info("  [Livy] GET /version OK: %s", (vr.text or "")[:160].strip())
            summary["livy_version"] = "ok"
        else:
            logging.warning("  [Livy] GET /version HTTP %s", vr.status_code)
            summary["livy_version"] = f"http_{vr.status_code}"
    except requests.RequestException as exc:
        logging.warning("  [Livy] GET /version 실패(미지원 배포일 수 있음): %s", exc)
        summary["livy_version"] = f"skip:{exc}"

    br = requests.get(f"{LIVY_URL}/batches", timeout=15)
    br.raise_for_status()
    bj = br.json()
    total = bj.get("total")
    if total is None and isinstance(bj.get("sessions"), list):
        total = len(bj["sessions"])
    logging.info(
        "  [Livy] GET /batches OK (HTTP %s) total=%s — Batch API 사용 가능",
        br.status_code,
        total,
    )
    summary["livy_batches"] = "ok"

    try:
        sr = requests.get(f"{LIVY_URL}/sessions", timeout=10)
        if sr.ok:
            sj = sr.json()
            st = sj.get("total", len(sj.get("sessions") or []))
            logging.info("  [Livy] GET /sessions OK total=%s", st)
            summary["livy_sessions"] = "ok"
        else:
            logging.warning("  [Livy] GET /sessions HTTP %s", sr.status_code)
            summary["livy_sessions"] = f"http_{sr.status_code}"
    except requests.RequestException as exc:
        logging.warning("  [Livy] GET /sessions 실패: %s", exc)
        summary["livy_sessions"] = f"fail:{exc}"

    # 2) YARN ResourceManager REST — Spark가 물리적으로 돌아가는 층
    yarn_url = _yarn_rm_web_url()
    try:
        yr = requests.get(f"{yarn_url.rstrip('/')}/ws/v1/cluster/info", timeout=10)
        if yr.ok:
            data = yr.json()
            cid = (data.get("clusterInfo") or {}).get("id", "?")
            logging.info("  [YARN] GET /ws/v1/cluster/info OK clusterId=%s url=%s", cid, yarn_url)
            summary["yarn_rm"] = "ok"
        else:
            logging.warning(
                "  [YARN] HTTP %s (Airflow→%s 불가할 수 있음. Livy 잡은 YARN 내부에서만 검증 가능)",
                yr.status_code,
                yarn_url,
            )
            summary["yarn_rm"] = f"http_{yr.status_code}"
    except requests.RequestException as exc:
        logging.warning(
            "  [YARN] 연결 실패: %s | url=%s — 워커에서 NodePort(30890) 방화벽이면 경고만",
            exc,
            yarn_url,
        )
        summary["yarn_rm"] = f"unreachable:{type(exc).__name__}"

    # 3) Spark History Server — 완료 앱 로그 UI
    sh_url = _spark_history_web_url()
    try:
        hr = requests.get(f"{sh_url.rstrip('/')}/api/v1/applications", params={"limit": 1}, timeout=10)
        if hr.ok:
            logging.info("  [Spark History] GET /api/v1/applications OK url=%s", sh_url)
            summary["spark_history"] = "ok"
        else:
            logging.warning("  [Spark History] HTTP %s url=%s", hr.status_code, sh_url)
            summary["spark_history"] = f"http_{hr.status_code}"
    except requests.RequestException as exc:
        logging.warning("  [Spark History] 연결 실패: %s url=%s", exc, sh_url)
        summary["spark_history"] = f"unreachable:{type(exc).__name__}"

    # 4) HDFS — WebHDFS GETFILESTATUS 로 Livy `file`·`jars` 경로 실제 존재 확인 (없으면 예외)
    nn_url = _hdfs_namenode_web_url()
    artifact_summary = verify_hdfs_livy_artifacts_exist(nn_url)
    summary.update(artifact_summary)
    summary["hdfs_nn"] = (
        "skipped_check" if artifact_summary.get("hdfs_job_py", "").startswith("skipped") else "ok_files_checked"
    )

    # 5) Postgres — Airflow 워커 기준 (Spark Executor는 동일 DB로 JDBC; 네트워크 경로는 별도)
    try:
        conn = BaseHook.get_connection(QUALITY_POSTGRES_CONN_ID)
        pg = PostgresHelper(conn_id=QUALITY_POSTGRES_CONN_ID)
        pg.execute_query("SELECT 1 AS ok", task_id="spark_stack_pg_ping", xcom_key=None)
        logging.info(
            "  [Postgres] Airflow→%s:%s DB 연결 OK (Spark는 YARN에서 동일 호스트로 JDBC)",
            conn.host,
            conn.port or 5432,
        )
        summary["postgres_airflow"] = "ok"
    except Exception as exc:
        logging.error("  [Postgres] 연결 실패: %s", exc)
        summary["postgres_airflow"] = f"fail:{exc}"
        raise

    logging.info(
        "══ Spark 실행 스택 점검 완료 요약: %s ══",
        summary,
    )
    return summary


def _fetch_livy_batch_log_tail(batch_id, size: int = 20) -> Optional[list]:
    """Livy GET /batches/{id}/log — 드라이버 기동 지연·실패 원인 힌트용."""
    try:
        lr = requests.get(
            f"{LIVY_URL}/batches/{batch_id}/log",
            params={"from": 0, "size": size},
            timeout=30,
        )
        if not lr.ok:
            logging.warning("Livy GET /batches/%s/log HTTP %s", batch_id, lr.status_code)
            return None
        data = lr.json()
        if isinstance(data, dict) and "log" in data:
            return data["log"] if isinstance(data["log"], list) else [str(data["log"])]
        if isinstance(data, list):
            return data
    except requests.RequestException as exc:
        logging.warning("Livy batch log fetch 실패: %s", exc)
        return None
    return None


def _format_livy_failure_message(batch_id, state: str, status: dict) -> str:
    """dead/error/killed 시 인라인 log + REST log + driver URL을 한 문자열로."""
    app_id = status.get("appId")
    driver_url = status.get("driverLogUrl")
    inline = status.get("log")
    if isinstance(inline, list):
        inline_tail = inline[-40:]
    else:
        inline_tail = [str(inline)] if inline else []

    rest = _fetch_livy_batch_log_tail(batch_id, size=300) or []
    rest_tail = rest[-80:] if rest else []

    parts = [
        f"state={state}",
        f"batch_id={batch_id}",
        f"appId={app_id}",
        f"driverLogUrl={driver_url}",
    ]
    if inline_tail:
        parts.append("--- Livy batch inline log (tail) ---")
        parts.extend(inline_tail)
    if rest_tail:
        parts.append("--- Livy GET /batches/{id}/log (tail) ---")
        parts.extend(rest_tail)
    if not inline_tail and not rest_tail:
        parts.append(
            "(로그 비어 있음 — YARN/Spark UI에서 appId 또는 Livy 서버 로그 확인)"
        )
    return "\n".join(parts)


def submit_spark_temperature_matching(target_date: str, suffix: str = "_spark",
                                      poll_interval: int = 30,
                                      max_wait: int = 1800) -> dict:
    """Livy REST API로 PySpark 온도 매칭 잡 제출 후 완료 대기"""
    from airflow.hooks.base import BaseHook

    verify_spark_execution_stack()

    # Airflow Connection에서 PG 접속 정보 획득
    conn = BaseHook.get_connection(QUALITY_POSTGRES_CONN_ID)
    jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port or 5432}/{conn.schema}"

    # _spark 테이블 사전 생성
    pg = PostgresHelper(conn_id=QUALITY_POSTGRES_CONN_ID)
    ensure_spark_tables_exist(pg, suffix)

    payload = {
        "file": LIVY_JOB_PATH,
        "args": [target_date, suffix, jdbc_url, conn.login, conn.password],
        "jars": [LIVY_JDBC_JAR],
        "name": f"ipi_temp_matching_{target_date}{suffix}",
        "conf": {
            "spark.executor.memory": "2g",
            "spark.driver.memory":   "1g",
            "spark.executor.cores":  "2",
        },
    }

    resp = requests.post(f"{LIVY_URL}/batches", json=payload, timeout=30)
    resp.raise_for_status()
    batch_body = resp.json()
    batch_id = batch_body["id"]
    logging.info(
        "🚀 Livy POST /batches 수락: HTTP %s | batch_id=%s | 날짜=%s | suffix=%s | 응답 keys=%s",
        resp.status_code,
        batch_id,
        target_date,
        suffix,
        list(batch_body.keys()),
    )
    logging.info(
        "→ Livy가 배치 등록을 수락했으면(batch id 부여) 잡 '수신'은 정상입니다. "
        "이후 state=starting 장시간은 보통 YARN 리소스/큐 또는 드라이버 기동 지연입니다.",
    )

    # 잡이 Livy에 등록됐는지 즉시 확인 (UI: 보통 %s/batch/%s 형태 — 버전별 상이)
    verify = requests.get(f"{LIVY_URL}/batches/{batch_id}", timeout=10)
    verify.raise_for_status()
    v = verify.json()
    logging.info(
        "✅ Livy 배치 조회 확인 GET /batches/%s: state=%s appId=%s driverLogUrl=%s",
        batch_id,
        v.get("state"),
        v.get("appId"),
        v.get("driverLogUrl"),
    )

    # 완료까지 폴링 (첫 회는 대기 없이 상태 확인)
    elapsed = 0
    while True:
        livy_resp = requests.get(f"{LIVY_URL}/batches/{batch_id}", timeout=10)
        livy_resp.raise_for_status()
        status = livy_resp.json()
        state = status.get("state", "unknown")
        app_id = status.get("appId")
        driver_log = status.get("driverLogUrl")
        # 매 회차마다 HTTP 응답 + 상태 (Livy가 살아 있고 폴링이 동작 중임을 로그로 증명)
        logging.info(
            "  Livy 폴링 OK | HTTP %s | batch_id=%s | state=%s | appId=%s | driverLogUrl=%s | 경과=%ss",
            livy_resp.status_code,
            batch_id,
            state,
            app_id,
            driver_log,
            elapsed,
        )
        if state == "starting" and elapsed >= 90 and not app_id:
            logging.warning(
                "  ⚠️ %ss 경과인데도 state=starting 이고 appId 없음 — YARN 큐/리소스 대기 또는 "
                "드라이버 기동 지연 가능. 클러스터 리소스·로그 확인.",
                elapsed,
            )
        if state == "starting" and elapsed >= 120 and (elapsed - 120) % 120 == 0:
            tail = _fetch_livy_batch_log_tail(batch_id)
            if tail:
                logging.info(
                    "  Livy 배치 로그 tail (최대 20줄): %s",
                    tail[-20:],
                )

        if state == "success":
            logging.info("✅ Spark 잡 완료 (id=%s)", batch_id)
            return {
                "batch_id": batch_id,
                "state": state,
                "target_date": target_date,
                "livy_batch_url": f"{LIVY_URL}/batches/{batch_id}",
                "app_id": status.get("appId"),
            }

        if state in ("dead", "error", "killed"):
            detail = _format_livy_failure_message(batch_id, state, status)
            logging.error("❌ Spark/Livy 배치 실패:\n%s", detail)
            raise RuntimeError(f"❌ Spark 잡 실패 (Livy batch_id={batch_id}, state={state})\n{detail}")

        if elapsed >= max_wait:
            raise TimeoutError(f"⏰ Spark 잡 타임아웃 (id={batch_id}, {elapsed}s)")

        time.sleep(poll_interval)
        elapsed += poll_interval


def process_single_date(target_date: str) -> dict:
    """단일 날짜 전체 처리"""
    extract_time = datetime.utcnow()
    pg = PostgresHelper(conn_id=QUALITY_POSTGRES_CONN_ID)
    try:
        df_good = extract_good_product_data(pg, target_date, target_date)
        df_osnd = extract_osnd_data(pg, target_date, target_date)
        df_temp = extract_temperature_data(pg, target_date, target_date)
        df_defect = extract_defect_code_data(pg)
        df_osnd_mapped = map_defect_codes(merge_osnd_data(df_osnd, transform_good_to_osnd_format(df_good)), df_defect)
        if len(df_osnd_mapped) == 0:
            return {"status": "success", "rows_processed": 0, "rows_inserted": 0, "processed_date": target_date}
        combined_rows = match_temperature_data(df_osnd_mapped, df_temp)
        if len(combined_rows) == 0:
            return {"status": "success", "rows_processed": 0, "rows_inserted": 0, "processed_date": target_date}
        load_result = load_data(pg, combined_rows, extract_time)
        logging.info(f"✅ [{target_date}] 완료: {len(combined_rows):,} rows, 메인={load_result['main_rows']:,}, 상세={load_result['detail_rows']:,}")
        return {"status": "success", "rows_processed": len(combined_rows), "main_rows": load_result['main_rows'], "detail_rows": load_result['detail_rows'], "processed_date": target_date}
    except Exception as e:
        logging.error(f"❌ [{target_date}] 실패: {e}", exc_info=True)
        return {"status": "failed", "error": str(e), "processed_date": target_date}

