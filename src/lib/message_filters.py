from core_data_modules.logging import Logger
from dateutil.parser import isoparse

log = Logger(__name__)


# TODO: Move to Core once adapted for and tested on a pipeline that supports multiple radio shows
class MessageFilters(object):
    @staticmethod
    def filter_test_messages(messages, test_run_key="test_run"):
        """
        Filters a list of messages for messages which aren't tagged as being test messages.
        
        :param messages: List of message objects to filter. 
        :type messages: list of TracedData
        :param test_run_key: Key in each TracedData of the test message tag.
                             TracedData objects td where td.get(test_run_key) == True are dropped.
        :type test_run_key: str
        :return: Filtered list.
        :rtype: list of TracedData
        """
        log.debug("Filtering out test messages...")
        filtered = [td for td in messages if not td.get(test_run_key, False)]
        log.info(f"Filtered out test messages. "
                 f"Returning {len(filtered)}/{len(messages)} messages.")
        return filtered

    @staticmethod
    def filter_empty_messages(messages, message_keys):
        """
        Filters a list of messages for objects which contain an answer in at least one of the given message_keys.
        
        :param messages: List of message objects to filter.
        :type messages: list of TracedData
        :param message_keys: Keys in each TracedData to search for a message.
        :type message_keys: list of str
        :return: Filtered list.
        :rtype: list of TracedData 
        """
        log.debug("Filtering out empty message objects...")
        filtered = []
        for td in messages:
            for message_key in message_keys:
                if message_key in td:
                    filtered.append(td)
                    continue
        log.info(f"Filtered out empty message objects. "
                 f"Returning {len(filtered)}/{len(messages)} messages.")
        return filtered

    @staticmethod
    def filter_time_range(messages, time_key, start_time_inclusive, end_time_inclusive):
        """
        Filters a list of messages for messages received within the given time range.

        :param messages: List of message objects to filter.
        :type messages: list of TracedData
        :param time_key: Key in each TracedData object that contains the time the message was sent.
                         The values must be strings in ISO 8601 format.
        :type time_key: str
        :param start_time_inclusive: Inclusive start time of the time range to keep.
                           Messages sent before this time will be dropped. 
        :type start_time_inclusive: datetime.datetime
        :param end_time_inclusive: Exclusive end time of the time range to keep.
                         Messages sent after this time will be dropped.
        :type end_time_inclusive: datetime.datetime
        :return: Filtered list.
        :rtype: list of TracedData
        """
        log.debug(f"Filtering out messages sent outside the time range "
                  f"{start_time_inclusive.isoformat()} to {end_time_inclusive.isoformat()}...")
        filtered = [td for td in messages if start_time_inclusive <= isoparse(td[time_key]) < end_time_inclusive]
        log.info(f"Filtered out messages sent outside the time range "
                 f"{start_time_inclusive.isoformat()} to {end_time_inclusive.isoformat()}. "
                 f"Returning {len(filtered)}/{len(messages)} messages.")
        return filtered

    @staticmethod
    def filter_noise(messages, message_key, noise_fn):
        """
        Filters a list of messages for messages which aren't noise.
        
        :param messages: List of message objects to filter.
        :type messages: list of TracedData
        :param message_key: Key in the TracedData of the value to test for noise.
        :type message_key: str
        :param noise_fn: Function which, given a value, returns whether this message is noise.
        :type noise_fn: function of str -> bool
        :return: Filtered list.
        :rtype: list of TracedData
        """
        log.debug("Filtering out messages identified as noise...")
        filtered = [td for td in messages if not noise_fn(td.get(message_key))]
        log.info(f"Filtered out messages identified as noise. "
                 f"Returning {len(filtered)}/{len(messages)} messages.")
        return filtered
