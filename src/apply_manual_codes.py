import time
from os import path

from core_data_modules.cleaners import Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.cleaners.location_tools import KenyaLocations
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaV2IO
from core_data_modules.util import TimeUtils

from src.lib import PipelineConfiguration
from src.lib.pipeline_configuration import CodeSchemes


class ApplyManualCodes(object):
    @staticmethod
    def make_location_code(scheme, clean_value):
        if clean_value == Codes.NOT_CODED:
            return scheme.get_code_with_control_code(Codes.NOT_CODED)
        else:
            return scheme.get_code_with_match_value(clean_value)

    @classmethod
    def _impute_location_codes(cls, user, data):
        for td in data:
            # Up to 1 location code should have been assigned in Coda. Search for that code,
            # ensuring that only 1 has been assigned or, if multiple have been assigned, that they are non-conflicting
            # control codes
            location_code = None

            for plan in PipelineConfiguration.LOCATION_CODING_PLANS:
                coda_code = plan.code_scheme.get_code_with_id(td[plan.coded_field]["CodeID"])
                if location_code is not None:
                    if not (
                            coda_code.code_id == location_code.code_id or coda_code.control_code == Codes.NOT_REVIEWED):
                        location_code = CodeSchemes.CONSTITUENCY.get_code_with_control_code(Codes.CODING_ERROR)
                elif coda_code.control_code != Codes.NOT_REVIEWED:
                    location_code = coda_code

            # If no code was found, then this location is still not reviewed.
            # Synthesise a NOT_REVIEWED code accordingly.
            if location_code is None:
                location_code = CodeSchemes.CONSTITUENCY.get_code_with_control_code(Codes.NOT_REVIEWED)

            # If a control code was found, set all other location keys to that control code,
            # otherwise convert the provided location to the other locations in the hierarchy.
            if location_code.code_type == "Control":
                for plan in PipelineConfiguration.LOCATION_CODING_PLANS:
                    td.append_data({
                        plan.coded_field: CleaningUtils.make_label_from_cleaner_code(
                            plan.code_scheme,
                            plan.code_scheme.get_code_with_control_code(location_code.control_code),
                            Metadata.get_call_location()
                        ).to_dict()
                    }, Metadata(user, Metadata.get_call_location(), time.time()))
            else:
                location = location_code.match_values[0]
                td.append_data({
                    "county_coded": CleaningUtils.make_label_from_cleaner_code(
                        CodeSchemes.COUNTY,
                        cls.make_location_code(CodeSchemes.COUNTY,
                                               KenyaLocations.county_for_location_code(location)),
                        Metadata.get_call_location()).to_dict(),
                    "constituency_coded": CleaningUtils.make_label_from_cleaner_code(
                        CodeSchemes.CONSTITUENCY,
                        cls.make_location_code(CodeSchemes.CONSTITUENCY,
                                               KenyaLocations.constituency_for_location_code(location)),
                        Metadata.get_call_location()).to_dict()
                }, Metadata(user, Metadata.get_call_location(), time.time()))

    @classmethod
    def apply_manual_codes(cls, user, data, coda_input_dir):
        # Merge manually coded radio show files into the cleaned dataset
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            rqa_messages = [td for td in data if plan.raw_field in td]
            coda_input_path = path.join(coda_input_dir, plan.coda_filename)

            f = None
            try:
                if path.exists(coda_input_path):
                    f = open(coda_input_path, "r")
                TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable_multi_coded(
                    user, rqa_messages, plan.id_field, {plan.coded_field: plan.code_scheme}, f)

                if plan.binary_code_scheme is not None:
                    if f is not None:
                        f.seek(0)
                    TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable(
                        user, rqa_messages, plan.id_field, {plan.binary_coded_field: plan.binary_code_scheme}, f)
            finally:
                if f is not None:
                    f.close()

        # At this point, the TracedData objects still contain messages for at most one week each.
        # Label the weeks for which there is no response as TRUE_MISSING.
        for td in data:
            missing_dict = dict()
            for plan in PipelineConfiguration.RQA_CODING_PLANS:
                if plan.raw_field not in td:
                    na_label = CleaningUtils.make_label_from_cleaner_code(
                        plan.code_scheme, plan.code_scheme.get_code_with_control_code(Codes.TRUE_MISSING),
                        Metadata.get_call_location()
                    )
                    missing_dict[plan.coded_field] = [na_label.to_dict()]

                    if plan.binary_code_scheme is not None:
                        na_label = CleaningUtils.make_label_from_cleaner_code(
                            plan.binary_code_scheme, plan.binary_code_scheme.get_code_with_control_code(Codes.TRUE_MISSING),
                            Metadata.get_call_location()
                        )
                        missing_dict[plan.binary_coded_field] = na_label.to_dict()

            td.append_data(missing_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        # Mark data that is noise as Codes.NOT_CODED
        for td in data:
            if td["noise"]:
                nc_dict = dict()
                for plan in PipelineConfiguration.RQA_CODING_PLANS:
                    if plan.coded_field not in td:
                        nc_label = CleaningUtils.make_label_from_cleaner_code(
                            plan.code_scheme, plan.code_scheme.get_code_with_control_code(Codes.NOT_CODED),
                            Metadata.get_call_location()
                        )
                        nc_dict[plan.coded_field] = [nc_label.to_dict()]

                        if plan.binary_code_scheme is not None:
                            nc_label = CleaningUtils.make_label_from_cleaner_code(
                                plan.binary_code_scheme, plan.binary_code_scheme.get_code_with_control_code(Codes.NOT_CODED),
                                Metadata.get_call_location()
                            )
                            nc_dict[plan.binary_coded_field] = nc_label.to_dict()

                td.append_data(nc_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        # Synchronise the control codes between the binary and reasons schemes:
        # Some RQA datasets have a binary scheme, which is always labelled, and a reasons scheme, which is only labelled
        # if there is an additional reason given. Importing those two schemes separately above caused the labels in
        # each scheme to go out of sync with each other, e.g. reasons can be NR when the binary *was* reviewed.
        # This block updates the reasons scheme in cases where only a binary label was set, by assigning the
        # label 'NC' if the binary label was set to a normal code, otherwise to be the same control code as the binary.
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            rqa_messages = [td for td in data if plan.raw_field in td]
            if plan.binary_code_scheme is not None:
                for td in rqa_messages:
                    binary_label = td[plan.binary_coded_field]
                    binary_code = plan.binary_code_scheme.get_code_with_id(binary_label["CodeID"])

                    binary_label_present = binary_label["CodeID"] != \
                                           plan.binary_code_scheme.get_code_with_control_code(
                                               Codes.NOT_REVIEWED).code_id

                    reasons_label_present = len(td[plan.coded_field]) > 1 or td[plan.coded_field][0][
                        "CodeID"] != \
                                            plan.code_scheme.get_code_with_control_code(
                                                Codes.NOT_REVIEWED).code_id

                    if binary_label_present and not reasons_label_present:
                        if binary_code.code_type == "Control":
                            control_code = binary_code.control_code
                            reasons_code = plan.code_scheme.get_code_with_control_code(control_code)

                            reasons_label = CleaningUtils.make_label_from_cleaner_code(
                                plan.code_scheme, reasons_code,
                                Metadata.get_call_location(), origin_name="Pipeline Code Synchronisation")

                            td.append_data(
                                {plan.coded_field: [reasons_label.to_dict()]},
                                Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
                            )
                        else:
                            assert binary_code.code_type == "Normal"

                            nc_label = CleaningUtils.make_label_from_cleaner_code(
                                plan.code_scheme, plan.code_scheme.get_code_with_control_code(Codes.NOT_CODED),
                                Metadata.get_call_location(), origin_name="Pipeline Code Synchronisation"
                            )
                            td.append_data(
                                {plan.coded_field: [nc_label.to_dict()]},
                                Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
                            )

        # Merge manually coded survey files into the cleaned dataset
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            f = None
            try:
                coda_input_path = path.join(coda_input_dir, plan.coda_filename)
                if path.exists(coda_input_path):
                    f = open(coda_input_path, "r")
                TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable(
                    user, data, plan.id_field, {plan.coded_field: plan.code_scheme}, f)
            finally:
                if f is not None:
                    f.close()

        # Not everyone will have answered all of the demographic flows.
        # Label demographic questions which had no responses as TRUE_MISSING.
        # Label data which is just the empty string as NOT_CODED.
        for td in data:
            missing_dict = dict()
            for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
                if plan.raw_field not in td:
                    na_label = CleaningUtils.make_label_from_cleaner_code(
                        plan.code_scheme, plan.code_scheme.get_code_with_control_code(Codes.TRUE_MISSING),
                        Metadata.get_call_location()
                    )
                    missing_dict[plan.coded_field] = na_label.to_dict()
                elif td[plan.raw_field] == "":
                    nc_label = CleaningUtils.make_label_from_cleaner_code(
                        plan.code_scheme, plan.code_scheme.get_code_with_control_code(Codes.NOT_CODED),
                        Metadata.get_call_location()
                    )
                    missing_dict[plan.coded_field] = nc_label.to_dict()
            td.append_data(missing_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        # Set constituency/county codes from the coded district field.
        cls._impute_location_codes(user, data)

        return data
