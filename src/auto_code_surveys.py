from os import path

from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.traced_data.io import TracedDataCodaV2IO
from core_data_modules.util import IOUtils

from src.lib.pipeline_configuration import PipelineConfiguration


class AutoCodeSurveys(object):
    SENT_ON_KEY = "sent_on"

    @classmethod
    def auto_code_surveys(cls, user, data, coda_output_dir):
        # Auto-code surveys
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.cleaner is not None:
                    CleaningUtils.apply_cleaner_to_traced_data_iterable(user, data, plan.raw_field, cc.coded_field,
                                                                        cc.cleaner, cc.code_scheme)

        # Output single-scheme answers to coda for manual verification + coding
        IOUtils.ensure_dirs_exist(coda_output_dir)
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            TracedDataCodaV2IO.compute_message_ids(user, data, plan.raw_field, plan.id_field)

            coda_output_path = path.join(coda_output_dir, plan.coda_filename)
            with open(coda_output_path, "w") as f:
                TracedDataCodaV2IO.export_traced_data_iterable_to_coda_2(
                    data, plan.raw_field, plan.time_field, plan.id_field,
                    {cc.coded_field: cc.code_scheme for cc in plan.coding_configurations},
                    f
                )

        # Note: no need to handle location in any special way on this project because it is not being auto-coded

        return data
