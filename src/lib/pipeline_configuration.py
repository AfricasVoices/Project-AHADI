import json
from urllib.parse import urlparse

from core_data_modules.cleaners import Codes, swahili
from core_data_modules.data_models import Scheme, validators
from dateutil.parser import isoparse


def _open_scheme(filename):
    with open(f"code_schemes/{filename}", "r") as f:
        firebase_map = json.load(f)
        return Scheme.from_firebase_map(firebase_map)


class CodeSchemes(object):
    S01E01_REASONS = _open_scheme("s01e01_reasons.json")
    S01E02_REASONS = _open_scheme("s01e02_reasons.json")
    S01E03_REASONS = _open_scheme("s01e03_reasons.json")
    S01E04_REASONS = _open_scheme("s01e04_reasons.json")

    GENDER = _open_scheme("gender.json")
    AGE = _open_scheme("age.json")
    LIVELIHOOD = _open_scheme("livelihood.json")

    # WS_CORRECT_DATASET = _open_scheme("ws_correct_dataset.json")


class CodingPlan(object):
    def __init__(self, raw_field, coded_field, coda_filename, cleaner=None, code_scheme=None, time_field=None,
                 run_id_field=None, icr_filename=None, analysis_file_key=None, id_field=None,
                 binary_code_scheme=None, binary_coded_field=None, binary_analysis_file_key=None):
        self.raw_field = raw_field
        self.coded_field = coded_field
        self.coda_filename = coda_filename
        self.icr_filename = icr_filename
        self.cleaner = cleaner
        self.code_scheme = code_scheme
        self.time_field = time_field
        self.run_id_field = run_id_field
        self.analysis_file_key = analysis_file_key
        self.binary_code_scheme = binary_code_scheme
        self.binary_coded_field = binary_coded_field
        self.binary_analysis_file_key = binary_analysis_file_key

        if id_field is None:
            id_field = "{}_id".format(self.raw_field)
        self.id_field = id_field


class PipelineConfiguration(object):
    RQA_CODING_PLANS = [
        CodingPlan(raw_field="rqa_s01e01_raw",
                   coded_field="rqa_s01e01_coded",
                   time_field="sent_on",
                   coda_filename="s01e01.json",
                   icr_filename="s01e01.csv",
                   run_id_field="rqa_s01e01_run_id",
                   analysis_file_key="rqa_s01e01_",
                   code_scheme=CodeSchemes.S01E01_REASONS),

        CodingPlan(raw_field="rqa_s01e02_raw",
                   coded_field="rqa_s01e02_coded",
                   time_field="sent_on",
                   coda_filename="s01e02.json",
                   icr_filename="s01e02.csv",
                   run_id_field="rqa_s01e02_run_id",
                   analysis_file_key="rqa_s01e02_",
                   code_scheme=CodeSchemes.S01E02_REASONS),

        CodingPlan(raw_field="rqa_s01e03_raw",
                   coded_field="rqa_s01e03_coded",
                   time_field="sent_on",
                   coda_filename="s01e03.json",
                   icr_filename="s01e03.csv",
                   run_id_field="rqa_s01e03_run_id",
                   analysis_file_key="rqa_s01e03_",
                   code_scheme=CodeSchemes.S01E03_REASONS),

        CodingPlan(raw_field="rqa_s01e04_raw",
                   coded_field="rqa_s01e04_coded",
                   time_field="sent_on",
                   coda_filename="s01e04.json",
                   icr_filename="s01e04.csv",
                   run_id_field="rqa_s01e04_run_id",
                   analysis_file_key="rqa_s01e04_",
                   code_scheme=CodeSchemes.S01E04_REASONS),
    ]

    LOCATION_CODING_PLANS = []

    SURVEY_CODING_PLANS = []
    SURVEY_CODING_PLANS.extend(LOCATION_CODING_PLANS)
    SURVEY_CODING_PLANS.extend([
        CodingPlan(raw_field="gender_raw",
                   coded_field="gender_coded",
                   time_field="gender_time",
                   coda_filename="gender.json",
                   analysis_file_key="gender",
                   cleaner=swahili.DemographicCleaner.clean_gender,
                   code_scheme=CodeSchemes.GENDER),

        CodingPlan(raw_field="age_raw",
                   coded_field="age_coded",
                   time_field="age_time",
                   coda_filename="age.json",
                   analysis_file_key="age",
                   cleaner=swahili.DemographicCleaner.clean_age,
                   code_scheme=CodeSchemes.AGE),

        CodingPlan(raw_field="livelihood_raw",
                   coded_field="livelihood_coded",
                   time_field="livelihood_time",
                   coda_filename="livelihood.json",
                   analysis_file_key="livelihood",
                   code_scheme=CodeSchemes.LIVELIHOOD)
    ])

    def __init__(self, rapid_pro_domain, rapid_pro_token_file_url, activation_flow_names, survey_flow_names,
                 rapid_pro_test_contact_uuids, phone_number_uuid_table, recovery_csv_urls, rapid_pro_key_remappings,
                 project_start_date, project_end_date, filter_test_messages,
                 flow_definitions_upload_url_prefix, drive_upload=None):
        """
        :param rapid_pro_domain: URL of the Rapid Pro server to download data from.
        :type rapid_pro_domain: str
        :param rapid_pro_token_file_url: GS URL of a text file containing the authorisation token for the Rapid Pro
                                         server.
        :type rapid_pro_token_file_url: str
        :param activation_flow_names: The names of the RapidPro flows that contain the radio show responses.
        :type: activation_flow_names: list of str
        :param survey_flow_names: The names of the RapidPro flows that contain the survey responses.
        :type: survey_flow_names: list of str
        :param rapid_pro_test_contact_uuids: Rapid Pro contact UUIDs of test contacts.
                                             Runs for any of those test contacts will be tagged with {'test_run': True},
                                             and dropped when the pipeline is in production mode.
        :type rapid_pro_test_contact_uuids: list of str
        :param phone_number_uuid_table: Configuration for the Firestore phone number <-> uuid table.
        :type phone_number_uuid_table: PhoneNumberUuidTable
        :param rapid_pro_key_remappings: List of rapid_pro_key -> pipeline_key remappings.
        :type rapid_pro_key_remappings: list of RapidProKeyRemapping
        :param project_start_date: When data collection started - all activation messages received before this date
                                   time will be dropped.
        :type project_start_date: datetime.datetime
        :param project_end_date: When data collection stopped - all activation messages received on or after this date
                                 time will be dropped.
        :type project_end_date: datetime.datetime
        :param filter_test_messages: Whether to filter out messages sent from the rapid_pro_test_contact_uuids
        :type filter_test_messages: bool
        :param flow_definitions_upload_url_prefix: The prefix of the GS URL to uploads serialised flow definitions to.
                                                   This prefix will be appended with the current datetime and the
                                                   ".json" file extension.
        :type flow_definitions_upload_url_prefix: str
        :param recovery_csv_urls: GS URLs to CSVs in Shaqadoon's recovery format, or None.
        :type recovery_csv_urls: list of str | None
        :param drive_upload: Configuration for uploading to Google Drive, or None.
                             If None, does not upload to Google Drive.
        :type drive_upload: DriveUploadPaths | None
        """
        self.rapid_pro_domain = rapid_pro_domain
        self.rapid_pro_token_file_url = rapid_pro_token_file_url
        self.activation_flow_names = activation_flow_names
        self.survey_flow_names = survey_flow_names
        self.rapid_pro_test_contact_uuids = rapid_pro_test_contact_uuids
        self.phone_number_uuid_table = phone_number_uuid_table
        self.recovery_csv_urls = recovery_csv_urls
        self.rapid_pro_key_remappings = rapid_pro_key_remappings
        self.project_start_date = project_start_date
        self.project_end_date = project_end_date
        self.filter_test_messages = filter_test_messages
        self.drive_upload = drive_upload
        self.flow_definitions_upload_url_prefix = flow_definitions_upload_url_prefix

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        rapid_pro_domain = configuration_dict["RapidProDomain"]
        rapid_pro_token_file_url = configuration_dict["RapidProTokenFileURL"]
        activation_flow_names = configuration_dict["ActivationFlowNames"]
        survey_flow_names = configuration_dict["SurveyFlowNames"]
        recovery_csv_urls = configuration_dict.get("RecoveryCSVURLs")
        rapid_pro_test_contact_uuids = configuration_dict["RapidProTestContactUUIDs"]

        phone_number_uuid_table = PhoneNumberUuidTable.from_configuration_dict(configuration_dict["PhoneNumberUuidTable"])

        rapid_pro_key_remappings = []
        for remapping_dict in configuration_dict["RapidProKeyRemappings"]:
            rapid_pro_key_remappings.append(RapidProKeyRemapping.from_configuration_dict(remapping_dict))

        project_start_date = isoparse(configuration_dict["ProjectStartDate"])
        project_end_date = isoparse(configuration_dict["ProjectEndDate"])

        filter_test_messages = configuration_dict["FilterTestMessages"]

        drive_upload_paths = None
        if "DriveUpload" in configuration_dict:
            drive_upload_paths = DriveUpload.from_configuration_dict(configuration_dict["DriveUpload"])

        flow_definitions_upload_url_prefix = configuration_dict["FlowDefinitionsUploadURLPrefix"]

        return cls(rapid_pro_domain, rapid_pro_token_file_url, activation_flow_names, survey_flow_names,
                   rapid_pro_test_contact_uuids, phone_number_uuid_table, recovery_csv_urls, rapid_pro_key_remappings,
                   project_start_date, project_end_date, filter_test_messages,
                   flow_definitions_upload_url_prefix, drive_upload_paths)

    @classmethod
    def from_configuration_file(cls, f):
        return cls.from_configuration_dict(json.load(f))
    
    def validate(self):
        validators.validate_string(self.rapid_pro_domain, "rapid_pro_domain")
        validators.validate_string(self.rapid_pro_token_file_url, "rapid_pro_token_file_url")

        validators.validate_list(self.activation_flow_names, "activation_flow_names")
        for i, activation_flow_name in enumerate(self.activation_flow_names):
            validators.validate_string(activation_flow_name, f"activation_flow_names[{i}]")

        validators.validate_list(self.survey_flow_names, "survey_flow_names")
        for i, survey_flow_name in enumerate(self.survey_flow_names):
            validators.validate_string(survey_flow_name, f"survey_flow_names[{i}]")

        if self.recovery_csv_urls is not None:
            validators.validate_list(self.recovery_csv_urls, "recovery_csv_urls")
            for i, recovery_csv_url in enumerate(self.recovery_csv_urls):
                validators.validate_string(recovery_csv_url, f"recovery_csv_urls[{i}]")

        validators.validate_list(self.rapid_pro_test_contact_uuids, "rapid_pro_test_contact_uuids")
        for i, contact_uuid in enumerate(self.rapid_pro_test_contact_uuids):
            validators.validate_string(contact_uuid, f"rapid_pro_test_contact_uuids[{i}]")

        assert isinstance(self.phone_number_uuid_table, PhoneNumberUuidTable)
        self.phone_number_uuid_table.validate()

        validators.validate_list(self.rapid_pro_key_remappings, "rapid_pro_key_remappings")
        for i, remapping in enumerate(self.rapid_pro_key_remappings):
            assert isinstance(remapping, RapidProKeyRemapping), \
                f"rapid_pro_key_mappings[{i}] is not of type RapidProKeyRemapping"
            remapping.validate()

        validators.validate_datetime(self.project_start_date, "project_start_date")
        validators.validate_datetime(self.project_end_date, "project_end_date")

        validators.validate_bool(self.filter_test_messages, "filter_test_messages")

        if self.drive_upload is not None:
            assert isinstance(self.drive_upload, DriveUpload), \
                "drive_upload is not of type DriveUpload"
            self.drive_upload.validate()

        validators.validate_string(self.flow_definitions_upload_url_prefix, "flow_definitions_upload_url_prefix")


class PhoneNumberUuidTable(object):
    def __init__(self, firebase_credentials_file_url, table_name):
        """
        :param firebase_credentials_file_url: GS URL to the private credentials file for the Firebase account where
                                                 the phone number <-> uuid table is stored.
        :type firebase_credentials_file_url: str
        :param table_name: Name of the data <-> uuid table in Firebase to use.
        :type table_name: str
        """
        self.firebase_credentials_file_url = firebase_credentials_file_url
        self.table_name = table_name

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        firebase_credentials_file_url = configuration_dict["FirebaseCredentialsFileURL"]
        table_name = configuration_dict["TableName"]

        return cls(firebase_credentials_file_url, table_name)

    def validate(self):
        validators.validate_url(self.firebase_credentials_file_url, "firebase_credentials_file_url", scheme="gs")
        validators.validate_string(self.table_name, "table_name")


class RapidProKeyRemapping(object):
    def __init__(self, is_activation_message, rapid_pro_key, pipeline_key):
        """
        :param is_activation_message: Whether this re-mapping contains an activation message (activation messages need
                                   to be handled differently because they are not always in the correct flow)
        :type is_activation_message: bool
        :param rapid_pro_key: Name of key in the dataset exported via RapidProTools.
        :type rapid_pro_key: str
        :param pipeline_key: Name to use for that key in the rest of the pipeline.
        :type pipeline_key: str
        """
        self.is_activation_message = is_activation_message
        self.rapid_pro_key = rapid_pro_key
        self.pipeline_key = pipeline_key
        
        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        is_activation_message = configuration_dict.get("IsActivationMessage", False)
        rapid_pro_key = configuration_dict["RapidProKey"]
        pipeline_key = configuration_dict["PipelineKey"]
        
        return cls(is_activation_message, rapid_pro_key, pipeline_key)
    
    def validate(self):
        validators.validate_bool(self.is_activation_message, "is_activation_message")
        validators.validate_string(self.rapid_pro_key, "rapid_pro_key")
        validators.validate_string(self.pipeline_key, "pipeline_key")


class DriveUpload(object):
    def __init__(self, drive_credentials_file_url, production_upload_path, messages_upload_path,
                 individuals_upload_path, traced_data_upload_path):
        """
        :param drive_credentials_file_url: GS URL to the private credentials file for the Drive service account to use
                                           to upload the output files.
        :type drive_credentials_file_url: str
        :param production_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                       production CSV to.
        :type production_upload_path: str
        :param messages_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                     messages analysis CSV to.
        :type messages_upload_path: str
        :param individuals_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                        individuals analysis CSV to.
        :type individuals_upload_path: str
        :param traced_data_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                        serialized TracedData from this pipeline run to.
        :type traced_data_upload_path: str
        """
        self.drive_credentials_file_url = drive_credentials_file_url
        self.production_upload_path = production_upload_path
        self.messages_upload_path = messages_upload_path
        self.individuals_upload_path = individuals_upload_path
        self.traced_data_upload_path = traced_data_upload_path

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        drive_credentials_file_url = configuration_dict["DriveCredentialsFileURL"]
        production_upload_path = configuration_dict["ProductionUploadPath"]
        messages_upload_path = configuration_dict["MessagesUploadPath"]
        individuals_upload_path = configuration_dict["IndividualsUploadPath"]
        traced_data_upload_path = configuration_dict["TracedDataUploadPath"]

        return cls(drive_credentials_file_url, production_upload_path, messages_upload_path,
                   individuals_upload_path, traced_data_upload_path)

    def validate(self):
        validators.validate_string(self.drive_credentials_file_url, "drive_credentials_file_url")
        assert urlparse(self.drive_credentials_file_url).scheme == "gs", "DriveCredentialsFileURL needs to be a gs " \
                                                                         "URL (i.e. of the form gs://bucket-name/file)"

        validators.validate_string(self.production_upload_path, "production_upload_path")
        validators.validate_string(self.messages_upload_path, "messages_upload_path")
        validators.validate_string(self.individuals_upload_path, "individuals_upload_path")
        validators.validate_string(self.traced_data_upload_path, "traced_data_upload_path")
