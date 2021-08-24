from datetime import datetime

import apache_beam as beam
from apache_beam.testing.test_stream import TestStream

start_timestamp = datetime(year=2000, month=1, day=1, hour=10).timestamp()


class StreamSamples:
    @classmethod
    def get_stream_without_timestamped_values(cls) -> TestStream:
        # streaming pipeline with test stream without timestamped values
        test_stream = TestStream()
        test_stream.advance_watermark_to(start_timestamp)  # set watermark to 08:00:00

        test_stream.add_elements([("A", 1.5)])
        test_stream.advance_watermark_to(start_timestamp+1)  # set watermark to 08:00:01

        test_stream.add_elements([("A", 1.2)])
        test_stream.add_elements([("A", 1.3)])
        test_stream.advance_watermark_to(start_timestamp+2)  # set watermark to 08:00:02

        test_stream.add_elements([("A", 1.4)])
        test_stream.advance_watermark_to(start_timestamp+3)  # set watermark to 08:00:03

        test_stream.add_elements([("A", 1.6)])
        test_stream.advance_watermark_to(start_timestamp+4)  # set watermark to 08:00:04

        test_stream.add_elements([("A", 1.7)])
        test_stream.advance_watermark_to(start_timestamp+5)  # set watermark to 08:00:05

        test_stream.advance_watermark_to_infinity()

        return test_stream

    @classmethod
    def get_late_data_stream(cls) -> TestStream:
        test_stream = TestStream()
        test_stream.advance_watermark_to(start_timestamp)
        test_stream.advance_processing_time(start_timestamp)

        test_stream.add_elements([beam.window.TimestampedValue(('A', 1.5), start_timestamp)])
        test_stream.advance_watermark_to(start_timestamp + 1)
        test_stream.advance_processing_time(1)

        test_stream.add_elements([
            beam.window.TimestampedValue(('A', 1.2), start_timestamp + 1),
            beam.window.TimestampedValue(('A', 1.3), start_timestamp + 1)
        ])
        test_stream.advance_watermark_to(start_timestamp + 2)
        test_stream.advance_processing_time(1)

        test_stream.add_elements([
            beam.window.TimestampedValue(('A', 1.4), start_timestamp + 2)])
        test_stream.advance_watermark_to(start_timestamp + 3)
        test_stream.advance_processing_time(1)

        test_stream.add_elements([
            beam.window.TimestampedValue(('A', 1.6), start_timestamp + 3)])

        test_stream.advance_watermark_to(start_timestamp + 8)
        test_stream.advance_processing_time(5)
        test_stream.add_elements([
            beam.window.TimestampedValue(('A', 1.7), start_timestamp + 8)])

        # late data
        test_stream.add_elements([
            beam.window.TimestampedValue(('A', 1.0009), start_timestamp),
            beam.window.TimestampedValue(('A', 1.0009), start_timestamp-1),
        ])

        test_stream.advance_watermark_to_infinity()
        return test_stream


class BatchSamples:

    @classmethod
    def get_timestamped_values(cls):
        values = [("A", 1.0), ("A", 1.2), ("A", 1.4), ("A", 1.6)]

        values_with_timestamps = [
            beam.window.TimestampedValue(value, (start_timestamp+i)) for value, i in zip(
                values, range(len(values)))]

        values_with_timestamps.append(
            beam.window.TimestampedValue(('A', 1.3), int(start_timestamp+1)))
        values_with_timestamps.append(
            beam.window.TimestampedValue(('A', 1.7), int(start_timestamp+10)))

        values_with_timestamps.sort(key=lambda x: x.timestamp)

        return values_with_timestamps
