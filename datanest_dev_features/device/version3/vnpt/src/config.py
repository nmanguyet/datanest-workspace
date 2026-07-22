from datetime import datetime, timedelta

FEATURE_ROOT = '/user/nguyetnguyen/device_sample/feature/inventory/device/version3'

# common
TAC_MAPPING_PATH = "/data/processed/mapping/mapping_device_tac_202507"
LAST_ACTIVATED_PATH = "/data/DS/ngocbui/processed/features/sub_fix/lad"
WINDOW_WEEKS = [12, 24, 48, 96]
SWITCH_GAP_WINDOWS_WEEKS = [12, 24, 48, 96]
DATE_FORMAT = "%Y-%m-%d"
SNAPSHOT_DATE_STR = "2024-09-29"

# build_imei_weekly_aggregation
IMEI_SMS_RAW_PATH = "/data/vnpt_v2/sms"
IMEI_VOICE_RAW_PATH = "/data/vnpt_v2/voice"
IMEI_WEEKLY_PATH = f"{FEATURE_ROOT}/device_imei_aggregation_weekly"

# build_device_imei_feature
DEVICE_IMEI_FEATURES_PATH = f"{FEATURE_ROOT}/device_imei_features_lxw"

# build_device_tac_feature
DEVICE_TAC_FEATURES_PATH = f"{FEATURE_ROOT}/device_tac_features_lxw"

# build_device_current_tac_feature
DEVICE_CURRENT_TAC_FEATURES_PATH = f"{FEATURE_ROOT}/device_current_tac_features_lxw"
CURRENT_TAC_WEEKS = 2
CURRENT_TAC_SUFFIX = f"l{CURRENT_TAC_WEEKS}w"
CURRENT_TAC_RANKS = [
    {"rank": 1, "prefix": "device_current"},
    {"rank": 2, "prefix": "device_2nd"},
]

# build_device_tac_behaviour_feature
DEVICE_TAC_BEHAVIOUR_FEATURES_PATH = f"{FEATURE_ROOT}/device_tac_behaviour_features_lxw"
DEVICE_CURRENT_TAC_COLUMN = 'device_current_tac_l2w'

# build_device_brand_behaviour_feature
DEVICE_BRAND_BEHAVIOUR_FEATURES_PATH = f"{FEATURE_ROOT}/device_brand_behaviour_features_lxw"
DEVICE_CURRENT_BRAND_COLUMN = 'device_current_brand_l2w'


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
