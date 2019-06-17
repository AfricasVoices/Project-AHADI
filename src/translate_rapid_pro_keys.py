from datetime import datetime

import pytz
from core_data_modules.logging import Logger
from core_data_modules.traced_data import Metadata
from core_data_modules.util import TimeUtils
from dateutil.parser import isoparse

log = Logger(__name__)


class TranslateRapidProKeys(object):
    @classmethod
    def set_show_ids(cls, user, data, pipeline_configuration):
        """
        Sets a show pipeline key for each message, using the presence of Rapid Pro value keys to determine which
        show each message belongs to.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to set the show ids of.
        :type data: iterable of TracedData
        :param pipeline_configuration: Pipeline configuration.
        :type pipeline_configuration: PipelineConfiguration
        """
        for td in data:
            show_dict = dict()

            for remapping in pipeline_configuration.rapid_pro_key_remappings:
                if not remapping.is_activation_message:
                    continue

                if remapping.rapid_pro_key in td:
                    assert "rqa_message" not in show_dict
                    show_dict["rqa_message"] = td[remapping.rapid_pro_key]
                    show_dict["show_pipeline_key"] = remapping.pipeline_key

            td.append_data(show_dict, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    @classmethod
    def _remap_radio_show_by_time_range(cls, user, data, time_key, show_pipeline_key_to_remap_to,
                                        range_start=None, range_end=None, time_to_adjust_to=None):
        """
        Remaps radio show messages received in the given time range to another radio show.

        Optionally adjusts the datetime of re-mapped messages to a constant.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to set the show ids of.
        :type data: iterable of TracedData
        :param time_key: Key in each TracedData of an ISO 8601-formatted datetime string to read the message sent on
                         time from.
        :type time_key: str
        :param show_pipeline_key_to_remap_to: Pipeline key to assign to messages received within the given time range.
        :type show_pipeline_key_to_remap_to: str
        :param range_start: Start datetime for the time range to remap radio show messages from, inclusive.
                            If None, defaults to the beginning of time.
        :type range_start: datetime | None
        :param range_end: End datetime for the time range to remap radio show messages from, exclusive.
                          If None, defaults to the end of time.
        :type range_end: datetime | None
        :param time_to_adjust_to: Datetime to assign to the 'sent_on' field of re-mapped shows.
                                  If None, re-mapped shows will not have timestamps re-adjusted.
        :type time_to_adjust_to: datetime | None
        """
        if range_start is None:
            range_start = pytz.utc.localize(datetime.min)
        if range_end is None:
            range_end = pytz.utc.localize(datetime.max)

        log.info(f"Remapping messages in time range {range_start.isoformat()} to {range_end.isoformat()} "
                 f"to show {show_pipeline_key_to_remap_to}...")

        remapped_count = 0
        for td in data:
            if time_key in td and range_start <= isoparse(td[time_key]) < range_end:
                remapped_count += 1

                remapped = {
                    "show_pipeline_key": show_pipeline_key_to_remap_to
                }
                if time_to_adjust_to is not None:
                    remapped[time_key] = time_to_adjust_to.isoformat()

                td.append_data(remapped,
                               Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

        log.info(f"Remapped {remapped_count} messages to show {show_pipeline_key_to_remap_to}")

    @classmethod
    def remap_radio_shows(cls, user, data, coda_input_dir):
        """
        Remaps radio shows which were in the wrong flow, and therefore have the wrong key/values set, to have the
        key/values they would have had if they had been received by the correct flow.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to move the radio show messages in.
        :type data: iterable of TracedData
        :param coda_input_dir: Directory to read coded coda files from.
        :type coda_input_dir: str
        """
        # No implementation needed yet, because no flow is yet to go wrong in production.
        pass

    @classmethod
    def remap_key_names(cls, user, data, pipeline_configuration):
        """
        Remaps key names.
        
        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to remap the key names of.
        :type data: iterable of TracedData
        :param pipeline_configuration: Pipeline configuration.
        :type pipeline_configuration: PipelineConfiguration
        """
        for td in data:
            remapped = dict()
               
            for remapping in pipeline_configuration.rapid_pro_key_remappings:
                if remapping.is_activation_message:
                    continue

                old_key = remapping.rapid_pro_key
                new_key = remapping.pipeline_key
                
                if old_key in td and new_key not in td:
                    remapped[new_key] = td[old_key]

            td.append_data(remapped, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    @classmethod
    def set_rqa_raw_keys_from_show_ids(cls, user, data):
        """
        Despite the earlier phases of this pipeline stage using a common 'rqa_message' field and then a
        'show_pipeline_key' field to identify which radio show a message belonged to, the rest of the pipeline still
        uses the presence of a raw field for each show to determine which show a message belongs to.
        This function translates from the new 'show_id' method back to the old 'raw field presence` method.
        
        TODO: Update the rest of the pipeline to use show_ids, and/or perform remapping before combining the datasets.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to set raw radio show message fields for.
        :type data: iterable of TracedData
        """
        for td in data:
            if "show_pipeline_key" in td:
                td.append_data({td["show_pipeline_key"]: td["rqa_message"]},
                               Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    @classmethod
    def translate_rapid_pro_keys(cls, user, data, pipeline_configuration, coda_input_dir):
        """
        Remaps the keys of rqa messages in the wrong flow into the correct one, and remaps all Rapid Pro keys to
        more usable keys that can be used by the rest of the pipeline.
        
        TODO: Break this function such that the show remapping phase happens in one class, and the Rapid Pro remapping
              in another?
        """
        # Set the show pipeline key for each message, using the presence of Rapid Pro value keys in the TracedData.
        # These are necessary in order to be able to remap radio shows and key names separately (because data
        # can't be 'deleted' from TracedData).
        cls.set_show_ids(user, data, pipeline_configuration)

        # Move rqa messages which ended up in the wrong flow to the correct one.
        cls.remap_radio_shows(user, data, coda_input_dir)

        # Remap the keys used by Rapid Pro to more usable key names that will be used by the rest of the pipeline.
        cls.remap_key_names(user, data, pipeline_configuration)

        # Convert from the new show key format to the raw field format still used by the rest of the pipeline.
        cls.set_rqa_raw_keys_from_show_ids(user, data)

        return data
