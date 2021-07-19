from typing import Any, Iterable, Tuple

import apache_beam as beam


class GetElementTimestamp(beam.DoFn):
    def __init__(self, print_pane_info: bool = False):
        self.print_pane_info = print_pane_info

    def process(self, element: Any,
                timestamp=beam.DoFn.TimestampParam,
                window=beam.DoFn.WindowParam,
                pane_info=beam.DoFn.PaneInfoParam
                ) -> Iterable[Tuple[str, str]]:

        try:
            timestamp_str = timestamp.to_utc_datetime()
        except Exception as err:
            print(err)
            timestamp_str = timestamp

        if window == beam.window.GlobalWindow():
            window_str = "The Global Window"
        else:
            window_str = f'Window ends at {window.max_timestamp().to_utc_datetime()}'

        if self.print_pane_info:
            yield (window_str,
                   f'The value : {element} has timestamp {timestamp_str} with Pane {pane_info}')
        else:
            yield str(
                {window_str}), f'The value : {element} has timestamp {timestamp_str}'


class PrettyPrint(beam.PTransform):
    def expand(self, pcoll):
        return (pcoll
            | beam.combiners.ToList().without_defaults()
            | beam.Map(lambda x: sorted(x, key=lambda tup: tup[0]))
            | beam.Map(lambda x: print(*x, sep="\n"))
        )
