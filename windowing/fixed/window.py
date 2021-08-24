import apache_beam as beam

from base_pipeline import run_pipeline, ShowElementInfo


def run_fixed_window(pipeline):
    pipeline | beam.Create([1, 2, 3]) | ShowElementInfo()


if __name__ == '__main__':
    run_pipeline(run_fixed_window)
