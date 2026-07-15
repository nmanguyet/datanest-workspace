from datetime import datetime, timedelta

IMEI_SMS_RAW_PATH = "/data/vnpt_v2/sms"
IMEI_VOICE_RAW_PATH = "/data/vnpt_v2/voice"
IMEI_WEEKLY_PATH = "/feature/weekly/imei/merge_2_sources_agg"
IMEI_WEEKLY_HC_SAMPLE_PATH = "/user/nguyetnguyen/features/hc_2025/imei_weekly"
BASE_FEATURES_PATH = "/user/nguyetnguyen/tmp/test_new_features/base_df/hc_2025_all_features"
TAC_MAPPING_PATH = "/data/processed/mapping/mapping_device_tac_202507"
LAST_ACTIVATED_PATH = "/data/DS/ngocbui/processed/features/sub_fix/lad"
NID_RAW_PATH = "/data/vnpt_v2/naid"


DEVICE_IMEI_FEATURES_HC_SAMPLE_PATH = '/user/nguyetnguyen/features/hc_2025/device_imei'
DEVICE_TAC_FEATURES_HC_SAMPLE_PATH = '/user/nguyetnguyen/features/hc_2025/device_tac'
DEVICE_CURRENT_TAC_FEATURES_HC_SAMPLE_PATH = '/user/nguyetnguyen/features/hc_2025/device_current_tac'
DEVICE_TAC_BEHAVIOUR_FEATURES_HC_SAMPLE_PATH = '/user/nguyetnguyen/features/hc_2025/device_tac_behaviour'
DEVICE_BRAND_BEHAVIOUR_FEATURES_HC_SAMPLE_PATH = '/user/nguyetnguyen/features/hc_2025/device_brand_behaviour'
NID_FEATURES_HC_SAMPLE_PATH = '/user/nguyetnguyen/features/hc_2025/nid'

DEVICE_IMEI_FEATURES_HC_SAMPLE_MERGED_PATH = '/user/nguyetnguyen/features/hc_2025/device_imei_merge_all'
DEVICE_TAC_FEATURES_HC_SAMPLE_MERGED_PATH = '/user/nguyetnguyen/features/hc_2025/device_tac_merge_all'
DEVICE_CURRENT_TAC_FEATURES_HC_SAMPLE_MERGED_PATH = '/user/nguyetnguyen/features/hc_2025/device_current_tac_merge_all'
DEVICE_TAC_BEHAVIOUR_FEATURES_HC_SAMPLE_MERGED_PATH = '/user/nguyetnguyen/features/hc_2025/device_tac_behaviour_merge_all'
DEVICE_BRAND_BEHAVIOUR_FEATURES_HC_SAMPLE_MERGED_PATH = '/user/nguyetnguyen/features/hc_2025/device_brand_behaviour_merge_all'
NID_FEATURES_HC_SAMPLE_MERGED_PATH = '/user/nguyetnguyen/features/hc_2025/nid_merge_all'

WINDOW_WEEKS = [12, 24, 48, 96]
SWITCH_GAP_WINDOWS_WEEKS = [12, 24, 58, 96]


DATE_FORMAT = "%Y-%m-%d"

SNAPSHOT_DATE_STR = "2024-09-29"


def configure(snapshot_date_str: str = None):
    global SNAPSHOT_DATE_STR, SNAPSHOT_DATE
    global START_DATE_LAST_ACTIVATED_DATE, START_DATE_IMEI_FEATURES
    global START_DATE_TAC_FEATURES, START_DATE_CURRENT_TAC_FEATURES
    global START_DATE_TAC_BEHAVIOUR_FEATURES, START_DATE_BRAND_BEHAVIOUR_FEATURES
    global START_DATE_NID_FEATURES

    if snapshot_date_str is not None:
        SNAPSHOT_DATE_STR = snapshot_date_str

    SNAPSHOT_DATE = datetime.strptime(SNAPSHOT_DATE_STR, DATE_FORMAT)
    START_DATE_LAST_ACTIVATED_DATE = (SNAPSHOT_DATE - timedelta(days=2 * 7)).strftime(DATE_FORMAT)
    START_DATE_IMEI_FEATURES = (SNAPSHOT_DATE - timedelta(days=104 * 7)).strftime(DATE_FORMAT)
    START_DATE_TAC_FEATURES = (SNAPSHOT_DATE - timedelta(days=104 * 7)).strftime(DATE_FORMAT)
    START_DATE_CURRENT_TAC_FEATURES = (SNAPSHOT_DATE - timedelta(days=12 * 7)).strftime(DATE_FORMAT)
    START_DATE_TAC_BEHAVIOUR_FEATURES = (SNAPSHOT_DATE - timedelta(days=104 * 7)).strftime(DATE_FORMAT)
    START_DATE_BRAND_BEHAVIOUR_FEATURES = (SNAPSHOT_DATE - timedelta(days=104 * 7)).strftime(DATE_FORMAT)
    START_DATE_NID_FEATURES = (SNAPSHOT_DATE - timedelta(days=104 * 7)).strftime(DATE_FORMAT)


configure()

CURRENT_TAC_WEEKS = 2
CURRENT_TAC_SUFFIX = f"l{CURRENT_TAC_WEEKS}w"
CURRENT_TAC_RANKS = [
    {"rank": 1, "prefix": "device_current"},
    {"rank": 2, "prefix": "device_2nd"},
]

DEVICE_CURRENT_TAC_COLUMN = 'device_current_tac_l2w'
DEVICE_CURRENT_BRAND_COLUMN = 'device_current_brand_l2w'
DEVICE_CURRENT_OS_COLUMN = 'device_current_os_l2w'

