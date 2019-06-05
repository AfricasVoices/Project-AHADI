import time
from os import path

from core_data_modules.cleaners import Codes, PhoneCleaner
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaV2IO
from core_data_modules.util import IOUtils

from src.lib.pipeline_configuration import CodeSchemes, PipelineConfiguration


class AutoCodeSurveys(object):
    SENT_ON_KEY = "sent_on"

    @classmethod
    def auto_code_surveys(cls, user, data, phone_uuid_table, coda_output_dir):
        # Auto-code surveys
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            if plan.cleaner is not None:
                CleaningUtils.apply_cleaner_to_traced_data_iterable(user, data, plan.raw_field, plan.coded_field,
                                                                    plan.cleaner, plan.code_scheme)

        # For any locations where the cleaners assigned a code to a sub district, set the district code to NC
        # (this is because only one column should have a value set in Coda)
        # TODO: Handle locations in Kenya
        # for td in data:
        #     if "mogadishu_sub_district_coded" in td:
        #         mogadishu_code_id = td["mogadishu_sub_district_coded"]["CodeID"]
        #         if CodeSchemes.MOGADISHU_SUB_DISTRICT.get_code_with_id(mogadishu_code_id).code_type == "Normal":
        #             nc_label = CleaningUtils.make_label_from_cleaner_code(
        #                 CodeSchemes.MOGADISHU_SUB_DISTRICT,
        #                 CodeSchemes.MOGADISHU_SUB_DISTRICT.get_code_with_control_code(Codes.NOT_CODED),
        #                 Metadata.get_call_location(),
        #             )
        #             td.append_data({"district_coded": nc_label.to_dict()},
        #                            Metadata(user, Metadata.get_call_location(), time.time()))

        # Create a look-up table of uuids to phone numbers for all the uuids in the dataset
        # TODO: Handle operators in Kenya
        # uuids = set()
        # for td in data:
        #     uuids.add(td["uid"])
        # uuid_to_phone_lut = phone_uuid_table.uuid_to_data_batch(uuids)

        # Set the operator codes for each message, using the uuid -> phone number look-up table
        # for td in data:
        #     operator_clean = PhoneCleaner.clean_operator(uuid_to_phone_lut[td["uid"]])
        #     if operator_clean == Codes.NOT_CODED:
        #         label = CleaningUtils.make_label_from_cleaner_code(
        #             CodeSchemes.SOMALIA_OPERATOR, CodeSchemes.SOMALIA_OPERATOR.get_code_with_control_code(Codes.NOT_CODED),
        #             Metadata.get_call_location()
        #         )
        #     else:
        #         label = CleaningUtils.make_label_from_cleaner_code(
        #             CodeSchemes.SOMALIA_OPERATOR, CodeSchemes.SOMALIA_OPERATOR.get_code_with_match_value(operator_clean),
        #             Metadata.get_call_location()
        #         )
        #     td.append_data({"operator_coded": label.to_dict()}, Metadata(user, Metadata.get_call_location(), time.time()))

        # Output single-scheme answers to coda for manual verification + coding
        IOUtils.ensure_dirs_exist(coda_output_dir)
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            if plan.raw_field == "location_raw":
                continue
            
            TracedDataCodaV2IO.compute_message_ids(user, data, plan.raw_field, plan.id_field)

            coda_output_path = path.join(coda_output_dir, plan.coda_filename)
            with open(coda_output_path, "w") as f:
                TracedDataCodaV2IO.export_traced_data_iterable_to_coda_2(
                    data, plan.raw_field, plan.time_field, plan.id_field, {plan.coded_field: plan.code_scheme}, f
                )

        # Output location scheme to coda for manual verification + coding
        # TODO: Handle locations in Kenya
        # output_path = path.join(coda_output_dir, "location.json")
        # TracedDataCodaV2IO.compute_message_ids(user, data, "location_raw", "location_raw_id")
        # with open(output_path, "w") as f:
        #     TracedDataCodaV2IO.export_traced_data_iterable_to_coda_2(
        #         data, "location_raw", "location_time", "location_raw_id",
        #         {"mogadishu_sub_district_coded": CodeSchemes.MOGADISHU_SUB_DISTRICT,
        #          "district_coded": CodeSchemes.SOMALIA_DISTRICT,
        #          "region_coded": CodeSchemes.SOMALIA_REGION,
        #          "state_coded": CodeSchemes.SOMALIA_STATE,
        #          "zone_coded": CodeSchemes.SOMALIA_ZONE}, f
        #     )

        return data
