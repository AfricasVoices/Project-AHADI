import time

from core_data_modules.cleaners import Codes
from core_data_modules.traced_data import Metadata


class AnalysisKeys(object):
    @staticmethod
    def set_matrix_keys(user, data, all_matrix_keys, scheme, coded_key, matrix_prefix=""):
        for td in data:
            matrix_d = dict()

            for label in td.get(coded_key, []):
                matrix_d[f"{matrix_prefix}{scheme.get_code_with_id(label['CodeID']).string_value}"] = Codes.MATRIX_1

            for key in all_matrix_keys:
                if key not in matrix_d:
                    matrix_d[key] = Codes.MATRIX_0

            td.append_data(matrix_d, Metadata(user, Metadata.get_call_location(), time.time()))
