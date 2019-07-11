import time
from os import path

from core_data_modules.cleaners import Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaV2IO

from src.lib import PipelineConfiguration
from src.lib.pipeline_configuration import CodingModes


class ApplyManualCodes(object):
    @classmethod
    def apply_manual_codes(cls, user, data, coda_input_dir):
        # Merge manually coded data into the cleaned dataset
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
            coda_input_path = path.join(coda_input_dir, plan.coda_filename)

            for cc in plan.coding_configurations:
                f = None
                try:
                    if path.exists(coda_input_path):
                        f = open(coda_input_path, "r")

                    if cc.coding_mode == CodingModes.SINGLE:
                        TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable(
                            user, data, plan.id_field, {cc.coded_field: cc.code_scheme}, f)
                    else:
                        TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable_multi_coded(
                            user, data, plan.id_field, {cc.coded_field: cc.code_scheme}, f)
                finally:
                    if f is not None:
                        f.close()

        # Label data for which there is no response as TRUE_MISSING.
        # Label data for which the response is the empty string as NOT_CODED.
        for td in data:
            missing_dict = dict()
            for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
                if plan.raw_field not in td:
                    for cc in plan.coding_configurations:
                        na_label = CleaningUtils.make_label_from_cleaner_code(
                            cc.code_scheme, cc.code_scheme.get_code_with_control_code(Codes.TRUE_MISSING),
                            Metadata.get_call_location()
                        ).to_dict()
                        missing_dict[cc.coded_field] = na_label if cc.coding_mode == CodingModes.SINGLE else [na_label]
                elif td[plan.raw_field] == "":
                    for cc in plan.coding_configurations:
                        nc_label = CleaningUtils.make_label_from_cleaner_code(
                            cc.code_scheme, cc.code_scheme.get_code_with_control_code(Codes.NOT_CODED),
                            Metadata.get_call_location()
                        ).to_dict()
                        missing_dict[cc.coded_field] = nc_label if cc.coding_mode == CodingModes.SINGLE else [nc_label]
            td.append_data(missing_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        # Mark data that is noise as Codes.NOT_CODED
        for td in data:
            if td["noise"]:
                nc_dict = dict()
                for plan in PipelineConfiguration.RQA_CODING_PLANS:
                    for cc in plan.coding_configurations:
                        if cc.coded_field not in td:
                            nc_label = CleaningUtils.make_label_from_cleaner_code(
                                cc.code_scheme, cc.code_scheme.get_code_with_control_code(Codes.NOT_CODED),
                                Metadata.get_call_location()
                            ).to_dict()
                            nc_dict[cc.coded_field] = nc_label if cc.coding_mode == CodingModes.SINGLE else [nc_label]
                td.append_data(nc_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        # Run code imputation functions
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
            if plan.code_imputation_function is not None:
                plan.code_imputation_function(user, data, plan.coding_configurations)

        return data
