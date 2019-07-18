import time

from core_data_modules.cleaners import Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.logging import Logger
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaV2IO

from src.lib import PipelineConfiguration
from src.lib.pipeline_configuration import CodeSchemes

log = Logger(__name__)


class _WSUpdate(object):
    def __init__(self, message, sent_on, source):
        self.message = message
        self.sent_on = sent_on
        self.source = source


class WSCorrection(object):
    @staticmethod
    def move_wrong_scheme_messages(user, data, coda_input_dir):
        log.info("Importing manually coded Coda files to '_WS' fields...")
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
            TracedDataCodaV2IO.compute_message_ids(user, data, plan.raw_field, plan.id_field + "_WS")
            with open(f"{coda_input_dir}/{plan.coda_filename}") as f:
                TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable(
                    user, data, plan.id_field + "_WS",
                    {f"{plan.coded_field}_WS_correct_dataset": CodeSchemes.WS_CORRECT_DATASET}, f
                )

        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            with open(f"{coda_input_dir}/{plan.coda_filename}") as f:
                TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable_multi_coded(
                    user, data, plan.id_field + "_WS",
                    {f"{plan.coded_field}_WS": plan.code_scheme}, f
                )

                if plan.binary_code_scheme is not None:
                    f.seek(0)
                    TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable(
                        user, data, plan.id_field + "_WS",
                        {f"{plan.binary_coded_field}_WS": plan.binary_code_scheme}, f
                    )

        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            with open(f"{coda_input_dir}/{plan.coda_filename}") as f:
                TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable(
                    user, data, plan.id_field + "_WS",
                    {f"{plan.coded_field}_WS": plan.code_scheme}, f
                )

        log.info("Checking for WS Coding Errors...")
        # Check for coding errors in the RQA datasets.
        for td in data:
            for plan in PipelineConfiguration.RQA_CODING_PLANS:
                rqa_codes = []
                for label in td.get(f"{plan.coded_field}_WS", []):
                    rqa_codes.append(plan.code_scheme.get_code_with_id(label["CodeID"]))
                if plan.binary_code_scheme is not None and f"{plan.binary_coded_field}_WS" in td:
                    label = td[f"{plan.binary_coded_field}_WS"]
                    rqa_codes.append(plan.binary_code_scheme.get_code_with_id(label["CodeID"]))

                has_ws_code_in_code_scheme = False
                for code in rqa_codes:
                    if code.control_code == Codes.WRONG_SCHEME:
                        has_ws_code_in_code_scheme = True

                has_ws_code_in_ws_scheme = False
                if f"{plan.coded_field}_WS_correct_dataset" in td:
                    has_ws_code_in_ws_scheme = CodeSchemes.WS_CORRECT_DATASET.get_code_with_id(
                        td[f"{plan.coded_field}_WS_correct_dataset"]["CodeID"]).code_type == "Normal"

                if has_ws_code_in_code_scheme != has_ws_code_in_ws_scheme:
                    log.debug(f"Coding Error: {plan.raw_field}: {td[plan.raw_field]}")
                    coding_error_dict = {
                        f"{plan.coded_field}_WS_correct_dataset":
                            CleaningUtils.make_label_from_cleaner_code(
                                CodeSchemes.WS_CORRECT_DATASET,
                                CodeSchemes.WS_CORRECT_DATASET.get_code_with_control_code(Codes.CODING_ERROR),
                                Metadata.get_call_location(),
                            ).to_dict()
                    }
                    td.append_data(coding_error_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        # Check for coding errors in the surveys datasets, except location, as this is handled differently below.
        for td in data:
            for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
                if plan.raw_field == "location_raw":
                    continue

                has_ws_code_in_code_scheme = False
                if f"{plan.coded_field}_WS" in td:
                    has_ws_code_in_code_scheme = plan.code_scheme.get_code_with_id(
                        td[f"{plan.coded_field}_WS"]["CodeID"]).control_code == Codes.WRONG_SCHEME

                has_ws_code_in_ws_scheme = False
                if f"{plan.coded_field}_WS_correct_dataset" in td:
                    has_ws_code_in_ws_scheme = CodeSchemes.WS_CORRECT_DATASET.get_code_with_id(
                        td[f"{plan.coded_field}_WS_correct_dataset"]["CodeID"]).code_type == "Normal"

                if has_ws_code_in_code_scheme != has_ws_code_in_ws_scheme:
                    log.debug(f"Coding Error: {plan.raw_field}: {td[plan.raw_field]}")
                    coding_error_dict = {
                        f"{plan.coded_field}_WS_correct_dataset":
                            CleaningUtils.make_label_from_cleaner_code(
                                CodeSchemes.WS_CORRECT_DATASET,
                                CodeSchemes.WS_CORRECT_DATASET.get_code_with_control_code(Codes.CODING_ERROR),
                                Metadata.get_call_location(),
                            ).to_dict()
                    }
                    td.append_data(coding_error_dict, Metadata(user, Metadata.get_call_location(), time.time()))
                    
        # Check for coding errors in the locations dataset.
        for td in data:
            location_codes = []
            for plan in PipelineConfiguration.LOCATION_CODING_PLANS:
                if f"{plan.coded_field}_WS" in td:
                    label = td[f"{plan.coded_field}_WS"]
                    location_codes.append(plan.code_scheme.get_code_with_id(label["CodeID"]))

            has_ws_code_in_code_scheme = False
            for code in location_codes:
                if code.control_code == Codes.WRONG_SCHEME:
                    has_ws_code_in_code_scheme = True

            has_ws_code_in_ws_scheme = False
            for plan in PipelineConfiguration.LOCATION_CODING_PLANS:
                if f"{plan.coded_field}_WS_correct_dataset" in td:
                    if CodeSchemes.WS_CORRECT_DATASET.get_code_with_id(
                            td[f"{plan.coded_field}_WS_correct_dataset"]["CodeID"]).code_type == "Normal":
                        has_ws_code_in_ws_scheme = True

            if has_ws_code_in_code_scheme != has_ws_code_in_ws_scheme:
                log.debug(f"Coding Error: location_raw: {td['location_raw']}")
                coding_error_dict = dict()
                for plan in PipelineConfiguration.LOCATION_CODING_PLANS:
                    coding_error_dict[f"{plan.coded_field}_WS_correct_dataset"] = \
                        CleaningUtils.make_label_from_cleaner_code(
                            CodeSchemes.WS_CORRECT_DATASET,
                            CodeSchemes.WS_CORRECT_DATASET.get_code_with_control_code(Codes.CODING_ERROR),
                            Metadata.get_call_location(),
                        ).to_dict()
                td.append_data(coding_error_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        # Construct a map from WS normal code id to the raw field that code indicates a requested move to.
        ws_code_to_raw_field_map = dict()
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
            if plan.ws_code is not None:
                ws_code_to_raw_field_map[plan.ws_code.code_id] = plan.raw_field

        # Group the TracedData by uid.
        data_grouped_by_uid = dict()
        for td in data:
            uid = td["uid"]
            if uid not in data_grouped_by_uid:
                data_grouped_by_uid[uid] = []
            data_grouped_by_uid[uid].append(td)

        # Perform the WS correction for each uid.
        log.info("Performing WS correction...")
        corrected_data = []  # List of TracedData with the WS data moved.
        for group in data_grouped_by_uid.values():
            log.debug(f"\n\nStarting re-map for {group[0]['uid']}...")
            for i, td in enumerate(group):
                log.debug(f"--------------td[{i}]--------------")
                for _plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
                    log.debug(f"{_plan.raw_field}: {td.get(_plan.raw_field)}")
                    log.debug(f"{_plan.time_field}: {td.get(_plan.time_field)}")

            # Find all the surveys data being moved.
            # (Note: we only need to check one td in this group because all the demographics are the same)
            td = group[0]
            survey_moves = dict()  # of source_field -> target_field
            for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
                if plan.raw_field not in td:
                    continue
                ws_code = CodeSchemes.WS_CORRECT_DATASET.get_code_with_id(td[f"{plan.coded_field}_WS_correct_dataset"]["CodeID"])
                if ws_code.code_type == "Normal":
                    log.debug(f"Detected redirect from {plan.raw_field} -> {ws_code_to_raw_field_map.get(ws_code.code_id, ws_code.code_id)} for message {td[plan.raw_field]}")
                    survey_moves[plan.raw_field] = ws_code_to_raw_field_map[ws_code.code_id]

            # Find all the RQA data being moved.
            rqa_moves = dict()  # of (index in group, source_field) -> target_field
            for i, td in enumerate(group):
                for plan in PipelineConfiguration.RQA_CODING_PLANS:
                    if plan.raw_field not in td:
                        continue
                    ws_code = CodeSchemes.WS_CORRECT_DATASET.get_code_with_id(td[f"{plan.coded_field}_WS_correct_dataset"]["CodeID"])
                    if ws_code.code_type == "Normal":
                        log.debug(f"Detected redirect from ({i}, {plan.raw_field}) -> {ws_code_to_raw_field_map.get(ws_code.code_id, ws_code.code_id)} for message {td[plan.raw_field]}")
                        rqa_moves[(i, plan.raw_field)] = ws_code_to_raw_field_map[ws_code.code_id]

            # Build a dictionary of the survey fields that haven't been moved, and cleared fields for those which have.
            survey_updates = dict()  # of raw_field -> updated value
            for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
                if plan.raw_field in survey_moves.keys():
                    # Data is moving
                    survey_updates[plan.raw_field] = []
                elif plan.raw_field in td:
                    # Data is not moving
                    survey_updates[plan.raw_field] = [_WSUpdate(td[plan.raw_field], td[plan.time_field], plan.raw_field)]

            # Build a list of the rqa fields that haven't been moved.
            rqa_updates = []  # of (field, value)
            for i, td in enumerate(group):
                for plan in PipelineConfiguration.RQA_CODING_PLANS:
                    if plan.raw_field in td:
                        if (i, plan.raw_field) in rqa_moves.keys():
                            # Data is moving
                            pass
                        else:
                            # Data is not moving
                            rqa_updates.append((plan.raw_field, _WSUpdate(td[plan.raw_field], td[plan.time_field], plan.raw_field)))

            # Add data moving from survey fields to the relevant survey_/rqa_updates
            raw_survey_fields = {plan.raw_field for plan in PipelineConfiguration.SURVEY_CODING_PLANS}
            raw_rqa_fields = {plan.raw_field for plan in PipelineConfiguration.RQA_CODING_PLANS}
            for plan in PipelineConfiguration.SURVEY_CODING_PLANS + PipelineConfiguration.RQA_CODING_PLANS:
                if plan.raw_field not in survey_moves:
                    continue
                target_field = survey_moves[plan.raw_field]
                update = _WSUpdate(td[plan.raw_field], td[plan.time_field], plan.raw_field)
                if target_field in raw_survey_fields:
                    survey_updates[target_field] = survey_updates.get(target_field, []) + [update]
                else:
                    assert target_field in raw_rqa_fields, f"Raw field '{target_field}' not in any coding plan"
                    rqa_updates.append((target_field, update))

            # Add data moving from survey fields to the relevant survey_/rqa_updates
            for (i, source_field), target_field in rqa_moves.items():
                for plan in PipelineConfiguration.SURVEY_CODING_PLANS + PipelineConfiguration.RQA_CODING_PLANS:
                    if plan.raw_field == source_field:
                        _td = group[i]
                        update = _WSUpdate(_td[plan.raw_field], _td[plan.time_field], plan.raw_field)
                        if target_field in raw_survey_fields:
                            survey_updates[target_field] = survey_updates.get(target_field, []) + [update]
                        else:
                            assert target_field in raw_rqa_fields, f"Raw field '{target_field}' not in any coding plan"
                            rqa_updates.append((target_field, update))

            # Re-format the survey updates to a form suitable for use by the rest of the pipeline
            flattened_survey_updates = {}
            for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
                if plan.raw_field in survey_updates:
                    plan_updates = survey_updates[plan.raw_field]

                    if len(plan_updates) > 0:
                        flattened_survey_updates[plan.raw_field] = "; ".join([u.message for u in plan_updates])
                        flattened_survey_updates[plan.time_field] = sorted([u.sent_on for u in plan_updates])[0]
                        flattened_survey_updates[f"{plan.raw_field}_source"] = "; ".join([u.source for u in plan_updates])
                    else:
                        flattened_survey_updates[plan.raw_field] = None
                        flattened_survey_updates[plan.time_field] = None
                        flattened_survey_updates[f"{plan.raw_field}_source"] = None

            # Hide the survey keys currently in the TracedData which have had data moved away.
            td.hide_keys({k for k, v in flattened_survey_updates.items() if v is None}.intersection(td.keys()),
                         Metadata(user, Metadata.get_call_location(), time.time()))

            # Update with the corrected survey data
            td.append_data({k: v for k, v in flattened_survey_updates.items() if v is not None},
                           Metadata(user, Metadata.get_call_location(), time.time()))

            # Hide all the RQA fields (they will be added back, in turn, in the next step).
            td.hide_keys({plan.raw_field for plan in PipelineConfiguration.RQA_CODING_PLANS}.intersection(td.keys()),
                         Metadata(user, Metadata.get_call_location(), time.time()))

            # For each rqa message, create a copy of this td, append the rqa message, and add this to the
            # list of TracedData.
            for target_field, update in rqa_updates:
                rqa_dict = {
                    target_field: update.message,
                    "sent_on": update.sent_on,
                    f"{target_field}_source": update.source
                }

                corrected_td = td.copy()
                corrected_td.append_data(rqa_dict, Metadata(user, Metadata.get_call_location(), time.time()))
                corrected_data.append(corrected_td)

                log.debug(f"----------Created TracedData--------------")
                for _plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
                    log.debug(f"{_plan.raw_field}: {corrected_td.get(_plan.raw_field)}")
                    log.debug(f"{_plan.time_field}: {corrected_td.get(_plan.time_field)}")

        return corrected_data
