import apache_beam as beam

from base_pipeline import run_pipeline


def main(pipeline):
    pipeline | beam.Create([1, 2, 3]) | beam.Map(print)


if __name__ == '__main__':
    run_pipeline(main)

