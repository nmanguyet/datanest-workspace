##
import pandas as pd
import re

from device.clean_tac_mapping.device_brand_model_mapping import (BRAND_MAPPING, SAMSUNG_MODEL_MAP, XIAOMI_MODEL_MAP, OPPO_MODEL_MAP,
                     VIVO_MODEL_MAP, HUAWEI_MODEL_MAP, HONOR_MODEL_MAP, REALME_MODEL_MAP,
                     MOTOROLA_MODEL_MAP, NOKIA_MODEL_MAP,ZTE_MODEL_MAP, GOOGLE_MODEL_MAP)


def _clean_model(model):
    model = str(model).lower().strip()
    model = re.sub(r"\s+", " ", model)
    return model


def _strip_network(model):
    return re.sub(r"\s+(4g|5g)$", "", model).strip()


def normalize_apple_model(model):
    model = str(model).lower().strip()

    model = re.sub(r'(\d+)(st|nd|rd|th)\s+(?:generation|gen)', r'\1', model)
    model = re.sub(r'\ba\d{4}\b', '', model)
    model = re.sub(r'\s+', ' ', model).strip()

    size = r'(?:38|40|41|42|44|45|46|49)'

    model = re.sub(r'^watch\s+(\d+)(?:\s+' + size + r')?$', r'watch series \1', model)
    model = re.sub(r'watch ultra(\d+)', r'watch ultra \1', model)
    model = re.sub(rf'\b{size}mm\b', '', model)

    for p in [r'watch ultra \d+', r'watch se \d+']:
        model = re.sub(rf'^({p})\s+{size}$', r'\1', model)

    model = re.sub(rf'^watch se\s+{size}$', 'watch se', model)

    model = re.sub(r'(iphone se \d|ipad .*?)\s+\d{4}$', r'\1', model)
    model = re.sub(r'ipad air (\d+) (\d+)$', r'ipad air \1 gen\2', model)
    model = re.sub(r'ipad pro (11|12\.9|13) (\d+)$', r'ipad pro \1 gen\2', model)

    model = {'watch se 2022': 'watch se 2'}.get(model, model)

    model = re.sub(r'\s+', ' ', model).strip()
    return 'unknown' if model in {'', 'model'} else model


def normalize_samsung_model(model):
    model = _clean_model(model)
    model = re.sub(r"\([^)]*\)", "", model)

    model = (model.replace("galxy", "galaxy")
                  .replace("filp", "flip")
                  .replace("fan edition", "fe")
                  .replace("+", " plus "))

    model = re.sub(r"\bsm[- ]?[a-z0-9\-]+\b|\b(?:gt|sgh|shv|shw|sch)\s+[a-z0-9]+\b", "", model)
    model = re.sub(r"\b[a-z]\d{3,4}[a-z]{1,3}\b", "", model)
    model = re.sub(r"[_/\-]+", " ", model)
    model = re.sub(r"\b(duos|dual sim|dual|ds|dsn|uw|se)\b", "", model)
    model = re.sub(r"\blte\b", "4g", model)

    model = re.sub(r"\b([as]\d+|tab s\d+)fe\b", r"\1 fe", model)
    model = re.sub(r"^(a|s|j|m|f|note|tab|watch|fold|flip)(\d)", r"galaxy \1\2", model)
    model = re.sub(r"^z\s+(fold|flip)", r"galaxy z \1", model)

    model = re.sub(r"\b(a|s|j|m|f)\s+(\d+)", r"\1\2", model)
    model = re.sub(r"\b(note|watch|tab)\s+(\d+)", r"\1\2", model)
    model = re.sub(r"\bz\s+(fold|flip)\s+(\d+)", r"z \1\2", model)

    model = re.sub(r"watch(?:42|46)mm\b", "watch", model)
    model = re.sub(r"\b(watch\d+(?:\s+classic)?)\s+4g\b", r"\1", model)
    model = re.sub(r"\b(40|41|42|43|44|45|46|47|49)(mm)?\b", "", model)

    model = re.sub(r"\b(4g|5g)(?:\s+\1)+\b", r"\1", model)
    model = re.sub(r"(galaxy z fold\d+)(?:\s+z fold\d+)+(?:\s+5g)?", r"\1", model)
    model = re.sub(r"(galaxy z flip\d+)(?:\s+z flip\d+)+(?:\s+5g)?", r"\1", model)

    model = re.sub(r"\bgalaxy fold(\d+)\b", r"z fold\1", model)
    model = re.sub(r"\bgalaxy\s+galaxy\b", "galaxy", model)

    model = re.sub(r"\s+", " ", model).strip()
    model = SAMSUNG_MODEL_MAP.get(model, model)

    return "" if model in {"", "unknown", "galaxy", "galaxy 3", "galaxy 25 5g", "n"} else model
def normalize_xiaomi_model(model):
    model = _clean_model(model)

    if mapped := XIAOMI_MODEL_MAP.get(model):
        return mapped

    model = re.sub(r"^note\s+", "redmi note ", model)
    model = model.replace('pro+', 'pro plus')
    model = model.replace('pro +', 'pro plus')
    model = model.replace("discovery edition", "discovery")

    if re.match(r"^k\d+", model):
        model = f"redmi {model}"

    model = re.sub(r"^redmi mix\b", "mix", model)
    model = re.sub(r"^redmi max\b", "mi max", model)

    model = re.sub(r"\s+", " ", model).strip()

    return None if model in {"unknown", "smartphone", "redmi", "17 pro max"} else model


def normalize_oppo_model(model):
    if pd.isna(model):
        return model

    model = _clean_model(model)
    model = model.replace('pro+', 'pro plus')
    model = model.replace('pro +', 'pro plus')

    mapped_model = OPPO_MODEL_MAP.get(model)
    if mapped_model:
        return mapped_model

    return model

def normalize_vivo_model(model):
    model = _clean_model(model)

    while model.startswith('vivo '):
        model = model[5:].strip()

    if model in {'smartphone', ''}:
        return 'unknown'
    
    mapped_model = VIVO_MODEL_MAP.get(model)
    if mapped_model:
        return mapped_model

    model = model.replace('pro+', 'pro plus')
    model = model.replace('pro +', 'pro plus')
    model = re.sub(r'neo(\d)', r'neo \1', model)
    model = re.sub(r'nex(\d)', r'nex \1', model)
    model = re.sub(r'([xyzv])\s+(\d)', r'\1\2', model)
    model = re.sub(r'nex(\d)', r'nex \1', model)
    model = re.sub(r'xplay(\d)', r'xplay \1', model)
    model = re.sub(r'([a-z0-9])(4g|5g)$', r'\1 \2', model)

    # normalize spaces
    model = re.sub(r'\s+', ' ', model).strip()

    return model

def normalize_huawei_model(model):
    model = _clean_model(model)
    model = model.replace(" pro plus", " pro+")
    model = re.sub(r"\s+", " ", model).strip()
    model = model.replace("mate 20 x", "mate 20x")
    model = model.replace("p8lite", "p8 lite")
    model = model.replace("ascend mate7", "ascend mate 7")
    model = model.replace('pro+', 'pro plus')
    model = model.replace('pro +', 'pro plus')

    if model in HUAWEI_MODEL_MAP:
        return HUAWEI_MODEL_MAP[model]

    return _strip_network(model)

def normalize_honor_model(model):
    model = _clean_model(model)
    model = model.replace(" pro plus", " pro+")
    model = re.sub(r"\s+", " ", model).strip()
    model = model.replace("mate 20 x", "mate 20x")
    model = model.replace("p8lite", "p8 lite")
    model = model.replace("ascend mate7", "ascend mate 7")
    model = model.replace('pro+', 'pro plus')

    if model in HONOR_MODEL_MAP:
        return HONOR_MODEL_MAP[model]

    return _strip_network(model)

def normalize_realme_model(model):
    model = _clean_model(model)
    model = model.replace('pro+', 'pro plus')
    model = model.replace('pro +', 'pro plus')

    if model in REALME_MODEL_MAP:
        return REALME_MODEL_MAP[model]

    return _strip_network(model)

def normalize_motorola_model(model):
    model = str(model).lower().strip()

    if model in MOTOROLA_MODEL_MAP:
        return MOTOROLA_MODEL_MAP[model]

    model = _clean_model(model)
    model = model.replace("(", "").replace(")", " ").replace('pro+', 'pro plus')
    model = model.replace('pro +', 'pro plus')

    model = _strip_network(model)
    return model.replace("(", "").replace(")", " ").strip()

def normalize_zte_model(model):
    model = str(model).lower().strip()

    if model in ZTE_MODEL_MAP:
        return ZTE_MODEL_MAP[model]

    model = _clean_model(model)
    model = model.replace('pro+', 'pro plus')
    model = model.replace('pro +', 'pro plus')
    model = re.sub(r"\bblade\s+", "blade ", model)
    return _strip_network(model)


def normalize_google_model(model):
    model = str(model).lower().strip()

    if model in GOOGLE_MODEL_MAP:
        return GOOGLE_MODEL_MAP[model]

    model = _clean_model(model)
    model = model.replace('pro+', 'pro plus')
    model = model.replace('pro +', 'pro plus')
    model = model.replace("xl", " xl")
    return _strip_network(model)

def normalize_nokia_model(model):
    model = str(model).lower().strip()

    if model in NOKIA_MODEL_MAP:
        return NOKIA_MODEL_MAP[model]

    model = _clean_model(model)
    model = model.replace('pro+', 'pro plus')
    model = model.replace('pro +', 'pro plus')
    model = model.replace("xl", " xl")
    return _strip_network(model)

def _apply_brand_normalizer(df, brands, normalizer):
    if isinstance(brands, str):
        mask = df["device_brand"].eq(brands)
    else:
        mask = df["device_brand"].isin(brands)
    result = df.loc[mask, "device_model"].map(normalizer)
    df.loc[mask, "device_model"] = result.fillna("unknown")


def clean_ready_data(df):

    df_clean = df.copy()

    df_clean['_original_brand_for_prefix'] = df_clean['device_brand'].astype(str).str.lower().str.strip()

    df_clean["device_brand"] = (
        df_clean["device_brand"]
        .fillna("other")
        .astype(str)
        .str.lower()
        .str.strip()
    )

    df_clean["device_model"] = (
        df_clean["device_model"]
        .fillna("")
        .astype(str)
        .str.lower()
        .str.strip()
    )

    df_clean["device_brand"] = (
        df_clean["device_brand"]
        .replace(BRAND_MAPPING)
    )

    def add_xiaomi_subbrand_prefix(row):
        current_brand = row["device_brand"]
        original_brand_for_prefix = row["_original_brand_for_prefix"]
        model = row["device_model"]

        if current_brand == 'xiaomi':
          if original_brand_for_prefix == 'redmi' and not model.startswith('redmi'):
              model = 'redmi ' + model

          elif original_brand_for_prefix == 'poco' and not model.startswith('poco'):
              model = 'poco ' + model

          elif original_brand_for_prefix.startswith('mi'):
              model = re.sub(r'^mi\s+', 'redmi ', model)

          elif original_brand_for_prefix == 'xiaomi' and model.startswith('mi'):
              model = 'redmi ' + model[3:]

        return model.strip()

    df_clean["device_model"] = df_clean.apply(add_xiaomi_subbrand_prefix, axis=1)

    def initial_model_cleanup(row):
        brand = str(row["device_brand"])
        model = str(row["device_model"])

        if "test imei" in model:
            return ""

        if model in {"none", "nan", ""}:
            return "unknown"

        pattern = rf'^{re.escape(brand)}\s+'
        model = re.sub(pattern, '', model).strip()

        return model

    df_clean["device_model"] = (
        df_clean.apply(initial_model_cleanup, axis=1)
    )

    df_clean = df_clean.drop(columns=["_original_brand_for_prefix"])

    df_clean["device_model"] = (
        df_clean["device_model"]
        .str.replace(r"\\+", " plus ", regex=True)
        .str.replace(r"[()]", " ", regex=True)
        .str.replace(r"[_/\\\\-]+ ", " ", regex=True)
        .str.replace(r"\\d+\.?\\d*\\s*inch", "", regex=True)
        .str.replace('"', "", regex=False)
        .str.replace("''", "", regex=False)
        .str.replace(r"\\s+", " ", regex=True)
        .str.strip()
    )

    variant_mapping = {
        "pro max": "promax",
        "pro-max": "promax",
        "5 g": "5g",
    }

    for src, dst in variant_mapping.items():
        df_clean["device_model"] = (
            df_clean["device_model"]
            .str.replace(src, dst, regex=False)
        )

    brand_normalizers = [
        ("apple", normalize_apple_model),
        ("samsung", normalize_samsung_model),
        ("xiaomi", normalize_xiaomi_model),
        (["oppo", "oneplus"], normalize_oppo_model),
        ("vivo", normalize_vivo_model),
        ("huawei", normalize_huawei_model),
        ("honor", normalize_honor_model),
        ("realme", normalize_realme_model),
        ("motorola", normalize_motorola_model),
        ("zte", normalize_zte_model),
        ("google", normalize_google_model),
        ("nokia", normalize_nokia_model),
    ]

    for brands, normalizer in brand_normalizers:
        _apply_brand_normalizer(df_clean, brands, normalizer)

    df_clean.loc[df_clean["device_model"] == "", "device_model"] = "unknown"

    df_clean["device_name"] = df_clean["device_brand"] + " " + df_clean["device_model"]
    df_clean["device_name"] = df_clean["device_name"].str.replace(r"\\s+", " ", regex=True).str.strip()

    return df_clean


def _get_brand_model_text(row):
    brand = str(row.get('device_brand', '')).strip().lower()
    model = str(row.get('device_model', '')).strip().lower()
    return brand, model, f"{brand} {model}"


def _compile_regex(keywords, extra_patterns=None):
    patterns = [rf"\b{re.escape(k)}\b" for k in keywords]
    if extra_patterns:
        patterns.extend(extra_patterns)
    return re.compile("|".join(patterns), re.IGNORECASE)


CATEGORY_PATTERNS = {
    "Tablet/Laptop": _compile_regex([
        'tablet', 'ipad', 'tab', 'pad', 'surface', 'zpad', 'chromebook', 'elitebook',
        'thinkpad', 'ideapad', 'laptop', 'vivotab', 'ellipsis', 'acer one', 'alldocube',
        'nook', 'toughbook', 'toughpad', 'gpad', 'qtab', 'latitude',
    ]),
    "IoT/Module/Industrial": _compile_regex([
        'module', 'cpe', 'datacard', 'gateway', 'router', 'modem', 'tracker', 'pos',
        'zebra', 'honeywell', 'urovo', 'getac', 'telit', 'quectel', 'fibocom', 'cinterion',
        'wingtech', 'industrial', 'tcp', 'd-link', 'tp-link', 'sonim', 'hotspot',
        'linkzone', 'mobile hotspot', 'printer', 'ax20', 'nighthawk', 'wanway', 'wistron',
        'sercomm', 'telular', 'relay', 'pax', 'resideo', 'guardian', 'home connect',
        'pocket wifi', 'mifi', 'zoombak', 'syncup', 'tcu', 'sierra wireless', 'sierra',
        'simcom', 'broadmobi', 'queclink', 'teltonika', 'harvilon', 'green packet',
        'franklin', 'novatel', 'tozed', 'zlt', 'zowee', 'netgear', 'aircard', 'airprime', 'orbic speed',
    ]),
    "Wearable": _compile_regex(
        ['watch', 'fit', 'wear', 'ticwatch', 'spacetalk', 'fossil', 'gizmowatch', 'summit'],
        [r"\bgear s\d*\b"],
    ),
    "Feature Phone": _compile_regex(
        ['flip', 'feature', 'sgh', 'corby', 'keystone', 'strive', 'guru', 'keypad', 'digno',
         'classic', 'rebel', 'jitterbug', '105', '106', '107', '108', '110', '112', '1280',
         '215', '220', '3100', '3310', '6303', 'c1-00', 'c1-01', 'c2-02', 'c2-03', 'c2-06', 'c2-05',
         'n70', 'n72', 'n76', 'n97', 'x1-01', 'e28', 'mp02', 'aligator', 'amoi', 'benq',
         'siemens', 'sagem', 'gfive', 'qmobile', 'masstel', 'gratina'],
        [r"\b[wtk]\d{2,3}[a-z]?\b", r"\bpg-\w+\b"],
    ),
    "Smartphone": _compile_regex([
        'iphone', 'galaxy', 'xperia', 'pixel', 'redmi', 'oppo', 'vivo', 'huawei', 'oneplus',
        'moto', 'lg', 'htc', 'note', 'poco', 'nex', 'xplay', 'neo', 'blade', 'mate', 'nova',
        'honor', 'realme', 'fold', 'lumia', 'desire', 'ascend', 'edge', 'prime', 'active',
        'ultra', 'liquid', 'zmax', 'grand', 'libero', 'max', 'prestige', 'prelude', 'tecno',
        'infinix', 'blu', 'nubia', 'itel', 'blackview', 'mi', 'meizu', 'hisense', 'zte', 'alcatel',
        'asus', 'lenovo', 'motorola', 'samsung', 'google', 'xiaomi', 'nokia', 'blackberry',
        'tcl', 'doogee', 'ulefone', 'nothing', 'fairphone', 'unihertz', 'cat', 'kyocera',
        'stylo', 'revvl', 'aristo', 'thinq', 'aquos', 'vandroid', 'symphony', 'primo', 'zuk',
        'benco', 'blackshark', 'gotron', 'armor', 'oukitel', 'umidigi', 'wiko', 'nuu',
        'oscal', 'cubot', 'mara', 'sugar', 'leeco', 'elephone', 'gionee', 'essential',
        'calypso', 'droid', 'mytouch', 'sensation', 'fiesta', 'celero', 'revvlry', 'orbic',
        'boeing', 'blackphone', 'arrows', 'aquaris', 'skyphone',
    ]),
}


_NUMERIC_SMARTPHONE_BRANDS = {'samsung', 'xiaomi', 'lg', 'motorola', 'huawei', 'sony', 'lenovo'}
_NUMERIC_FEATURE_BRANDS = {'nokia', 'itel', 'masstel', 'sagem', 'siemens', 'aligator'}


def classify_device(row):
    brand, model, full_text = _get_brand_model_text(row)

    if not brand or not model or 'n/a' in model or 'n/a' in brand:
        return 'Other/Check_Manually'
    if model == 'unknown' or brand == 'unknown' or (brand == 'other' and model == 'other unknown'):
        return 'Other/Check_Manually'

    if brand == 'amazon':
        if 'phone' in model:
            return 'Smartphone'
        if any(k in model for k in ['echo', 'dot', 'show', 'fire tv', 'firestick', 'plug', 'alexa']):
            return 'IoT/Module/Industrial'
        return 'Tablet/Laptop'

    if 'nook' in model or 'nook' in brand or 'barnes and noble' in brand:
        return 'Tablet/Laptop'

    for category, regex in CATEGORY_PATTERNS.items():
        if regex.search(full_text):
            return category

    if re.search(r'\d{6,}', model):
        return 'IoT/Module/Industrial'

    if re.search(r'\b\d{3,4}\b', model):
        if brand in _NUMERIC_SMARTPHONE_BRANDS:
            return 'Smartphone'
        if brand in _NUMERIC_FEATURE_BRANDS:
            return 'Feature Phone'

    return 'Other/Check_Manually'


ANDROID_BRANDS = {
    'samsung', 'xiaomi', 'redmi', 'poco', 'oppo', 'vivo', 'realme', 'oneplus', 'moto', 'motorola',
    'huawei', 'honor', 'lg', 'htc', 'sony', 'xperia', 'zte', 'meizu', 'asus', 'lenovo', 'tecno',
    'infinix', 'itel', 'nokia', 'blackberry', 'tcl', 'google', 'pixel', 'alcatel', 'oukitel',
    'umidigi', 'wiko', 'blackview', 'cubot', 'oscal', 'benco', 'sharp', 'arrows', 'yota',
}

WINDOWS_MOBILE_KEYWORDS = {'windows phone', 'windows mobile', 'for windows', 'lumia', 'qtek', 'pu10', 'pocket pc'}

KAIOS_KEYWORDS = {'f320b', 'f120b', 'f220b', 'f300b', '8110 4g', 'kaios', 'jiophone', 'lyf f'}

WEARABLE_OS_KEYWORDS = {'galaxy watch', 'pixel watch', 'ticwatch', 'wearos', 'wear os'}

_LAPTOP_KEYWORDS = {'thinkpad', 'ideapad', 'latitude', 'elitebook', 'macbook'}


def classify_os(row, device_type=None):
    brand, model, full_text = _get_brand_model_text(row)

    if not brand or not model or 'n/a' in model or 'n/a' in brand or model == 'unknown':
        return 'Unknown'

    if 'apple' in brand or 'iphone' in full_text or 'ipad' in full_text:
        if device_type == 'Tablet/Laptop' or 'ipad' in full_text:
            return 'iPadOS'
        if device_type == 'Wearable' or 'watch' in full_text:
            return 'watchOS'
        return 'iOS'

    if any(k in full_text for k in WINDOWS_MOBILE_KEYWORDS):
        return 'Windows Phone / Windows Mobile'

    if brand == 'amazon' and 'phone' not in model and not any(k in model for k in ['echo', 'dot', 'show', 'fire tv', 'firestick']):
        return 'KindleOS'
    if 'nook' in full_text or 'barnes and noble' in brand:
        return 'NookOS'

    if device_type == 'Tablet/Laptop':
        if 'chromebook' in full_text:
            return 'ChromeOS'
        if any(k in full_text for k in _LAPTOP_KEYWORDS):
            return 'Windows / macOS'

    if any(k in full_text for k in KAIOS_KEYWORDS):
        return 'KaiOS'

    if device_type == 'Wearable' or 'watch' in full_text or 'gear s' in full_text:
        if 'gear s' in full_text or 'gear' in full_text:
            return 'TizenOS'
        if any(k in full_text for k in WEARABLE_OS_KEYWORDS):
            return 'WearOS (Android)'
        return 'Proprietary Wearable OS'

    if device_type == 'Feature Phone':
        return 'Proprietary (Feature Phone)'

    if device_type == 'IoT/Module/Industrial':
        return 'Proprietary / Embedded OS'

    if device_type in ('Smartphone', 'Tablet/Laptop') or brand in ANDROID_BRANDS:
        return 'Android'

    return 'Unknown/Proprietary'


def _is_prefix_overlap(a, b):
    a, b = a.strip().lower(), b.strip().lower()
    if a == b:
        return False
    wa, wb = a.split(), b.split()
    min_w = min(len(wa), len(wb))
    if wa[:min_w] == wb[:min_w]:
        return True
    if len(wa) == len(wb) and min_w > 1 and wa[:min_w - 1] == wb[:min_w - 1]:
        return True
    return False



def dedup_tac_prefix(df):
    kept = []
    for tac, group in df.groupby(level=0):
        names = group['device_name'].tolist()
        drop = set()
        for i, ni in enumerate(names):
            for j, nj in enumerate(names):
                if i != j and _is_prefix_overlap(ni, nj):
                    shorter = ni if len(ni) < len(nj) else nj
                    drop.add(shorter)
        kept.append(group[~group['device_name'].isin(drop)])
    return pd.concat(kept)


##
df_raw = pd.read_csv('/Users/anhnguyet/Documents/dev_fts/clean_tac_mapping/curated_tac_imei.csv')
df_ready_final = clean_ready_data(df_raw)
df_ready_final['device_category'] = df_ready_final.apply(classify_device, axis=1)
df_ready_final['device_os'] = df_ready_final.apply(classify_os, axis=1)
df_ready_final['device_model'] = df_ready_final['device_name']
df_ready_final['device_model'] = (
    df_ready_final['device_model']
    .str.replace(r'\b[345]g\b', '', case=False, regex=True)
    .str.strip()     
    .str.replace(r'\s+', ' ', regex=True) 
)
df_ready_final = dedup_tac_prefix(df_ready_final)

# df_ready_final.to_csv('/Users/anhnguyet/Documents/dev_fts/clean_tac_mapping/curated_tac_imei_cleaned_v1.csv', index=False)
##
df_dup = (
    df_ready_final
    .reset_index()
    .drop_duplicates(subset=['tac', 'device_brand', 'device_model'])
    .groupby(['tac']).size().reset_index(name='tac_count')
    .query('tac_count > 1')                               
    .merge(df_ready_final, on='tac', how='left')
    .sort_values(['tac', 'device_brand', 'device_model'])
)
df_dup
# %%
df_ready_final.shape, df_raw.shape

# %%
tac_final = df_ready_final['tac'].to_list()
tac_original = df_raw['tac'].to_list()

for t in tac_final:
    if t not in tac_original:
        print(f"New TAC: {t}")
        
for t in tac_original:
    if t not in tac_final:
        print(f"Missing TAC: {t}")
# %%
df_ready_final
# %%
