import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from typing import Callable, Optional, Any, Tuple, Iterable


class ShowElementInfo(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | beam.ParDo(GetElementInfo())
            | beam.Map(print)
        )


class GetElementInfo(beam.DoFn):

    def process(self, element: Any,
                timestamp=beam.DoFn.TimestampParam,
                window=beam.DoFn.WindowParam,
                pane_info=beam.DoFn.PaneInfoParam
                ) -> Iterable[Tuple[str, str]]:

        element_timestamp = self._get_timestamp(timestamp)
        window_start_timestamp = self._get_timestamp(window.start)
        window_end_timestamp = self._get_timestamp(window.max_timestamp())

        window_info = f"{window}, starts: {window_start_timestamp}, " \
                      f"ends: {window_end_timestamp}"

        yield f"The value: {element} has timestamp {element_timestamp}, " \
              f"window: {window_info}, " \
              f"pane: {pane_info}"

    def _get_timestamp(self, timestamp):
        try:
            timestamp.to_utc_datetime()
        # error occurs for negative timestamp Timestamp(-9223372036854.775000)
        # because it's impossible to get the utc datetime
        except OverflowError:
            timestamp = timestamp

        return timestamp


def run_pipeline(pipeline: Callable,
                 options: Optional[PipelineOptions] = PipelineOptions()):

    with beam.Pipeline(options=options) as p:
        pipeline(p)


