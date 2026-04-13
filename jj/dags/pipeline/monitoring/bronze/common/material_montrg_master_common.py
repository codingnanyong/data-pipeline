"""하위 호환: `material_montrg_master` 패키지 재노출.

새 코드는 `dags.pipeline.monitoring.bronze.common.material_montrg_master` 를 직접 import 해도 됩니다.
"""
from dags.pipeline.monitoring.bronze.common.material_montrg_master import *  # noqa: F403, F401
