import json

from core_data_modules.data_models import Scheme


def _open_scheme(filename):
    with open(f"code_schemes/{filename}", "r") as f:
        firebase_map = json.load(f)
        return Scheme.from_firebase_map(firebase_map)


class CodeSchemes(object):
    S01E01_REASONS = _open_scheme("s01e01_reasons.json")
    S01E02_REASONS = _open_scheme("s01e02_reasons.json")
    S01E03_REASONS = _open_scheme("s01e03_reasons.json")
    S01E04_REASONS = _open_scheme("s01e04_reasons.json")

    CONSTITUENCY = _open_scheme("constituency.json")
    COUNTY = _open_scheme("county.json")
    GENDER = _open_scheme("gender.json")
    AGE = _open_scheme("age.json")
    LIVELIHOOD = _open_scheme("livelihood.json")

    UNDERSTANDING = _open_scheme("understanding.json")
    INCLUSION = _open_scheme("inclusion.json")
    TRUST = _open_scheme("trust.json")
    POSITIVE_CHANGES = _open_scheme("positive_changes.json")

    # WS_CORRECT_DATASET = _open_scheme("ws_correct_dataset.json")
