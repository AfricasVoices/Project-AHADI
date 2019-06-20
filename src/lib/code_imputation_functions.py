import time

from core_data_modules.cleaners import Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.cleaners.location_tools import KenyaLocations
from core_data_modules.traced_data import Metadata
from core_data_modules.util import TimeUtils

from src.lib.code_schemes import CodeSchemes


def make_location_code(scheme, clean_value):
    if clean_value == Codes.NOT_CODED:
        return scheme.get_code_with_control_code(Codes.NOT_CODED)
    else:
        return scheme.get_code_with_match_value(clean_value)


def impute_yes_no_reasons_codes(user, data, coding_configurations):
    # Synchronise the control codes between the binary and reasons schemes:
    # Some RQA datasets have a binary scheme, which is always labelled, and a reasons scheme, which is only labelled
    # if there is an additional reason given. Importing those two schemes separately above caused the labels in
    # each scheme to go out of sync with each other, e.g. reasons can be NR when the binary *was* reviewed.
    # This block updates the reasons scheme in cases where only a binary label was set, by assigning the
    # label 'NC' if the binary label was set to a normal code, otherwise to be the same control code as the binary.
    binary_configuration = coding_configurations[0]
    reasons_configuration = coding_configurations[1]

    # TODO: Switch to using CodingModes.SINGLE/MULTIPLE once configuration is being set in configuration json
    #       rather than in pipeline_configuration.py
    assert binary_configuration.coding_mode == "single"
    assert reasons_configuration.coding_mode == "multiple"

    for td in data:
        binary_label = td[binary_configuration.coded_field]
        binary_code = binary_configuration.code_scheme.get_code_with_id(binary_label["CodeID"])

        binary_label_present = \
            binary_label["CodeID"] != binary_configuration.code_scheme.get_code_with_control_code(
                Codes.NOT_REVIEWED).code_id

        reasons_label_present = \
            len(td[reasons_configuration.coded_field]) > 1 or \
            td[reasons_configuration.coded_field][0][
                "CodeID"] != reasons_configuration.code_scheme.get_code_with_control_code(Codes.NOT_REVIEWED).code_id

        if binary_label_present and not reasons_label_present:
            if binary_code.code_type == "Control":
                control_code = binary_code.control_code
                reasons_code = reasons_configuration.code_scheme.get_code_with_control_code(control_code)

                reasons_label = CleaningUtils.make_label_from_cleaner_code(
                    reasons_configuration.code_scheme, reasons_code,
                    Metadata.get_call_location(), origin_name="Pipeline Code Synchronisation")

                td.append_data(
                    {reasons_configuration.coded_field: [reasons_label.to_dict()]},
                    Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
                )
            else:
                assert binary_code.code_type == "Normal"

                nc_label = CleaningUtils.make_label_from_cleaner_code(
                    reasons_configuration.code_scheme,
                    reasons_configuration.code_scheme.get_code_with_control_code(Codes.NOT_CODED),
                    Metadata.get_call_location(), origin_name="Pipeline Code Synchronisation"
                )
                td.append_data(
                    {reasons_configuration.coded_field: [nc_label.to_dict()]},
                    Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
                )


def impute_location_codes(user, data, location_configurations):
    for td in data:
        # Up to 1 location code should have been assigned in Coda. Search for that code,
        # ensuring that only 1 has been assigned or, if multiple have been assigned, that they are non-conflicting
        # control codes
        location_code = None

        for cc in location_configurations:
            coda_code = cc.code_scheme.get_code_with_id(td[cc.coded_field]["CodeID"])
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
            for cc in location_configurations:
                td.append_data({
                    cc.coded_field: CleaningUtils.make_label_from_cleaner_code(
                        cc.code_scheme,
                        cc.code_scheme.get_code_with_control_code(location_code.control_code),
                        Metadata.get_call_location()
                    ).to_dict()
                }, Metadata(user, Metadata.get_call_location(), time.time()))
        else:
            location = location_code.match_values[0]
            td.append_data({
                "county_coded": CleaningUtils.make_label_from_cleaner_code(
                    CodeSchemes.COUNTY,
                    make_location_code(CodeSchemes.COUNTY,
                                       KenyaLocations.county_for_location_code(location)),
                    Metadata.get_call_location()).to_dict(),
                "constituency_coded": CleaningUtils.make_label_from_cleaner_code(
                    CodeSchemes.CONSTITUENCY,
                    make_location_code(CodeSchemes.CONSTITUENCY,
                                       KenyaLocations.constituency_for_location_code(location)),
                    Metadata.get_call_location()).to_dict()
            }, Metadata(user, Metadata.get_call_location(), time.time()))
