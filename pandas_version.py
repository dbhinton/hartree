import apache_beam as beam
from apache_beam.transforms.util import CoGroupByKey

# Create a pipeline
with beam.Pipeline() as pipeline:
    # Read dataset1 and dataset2
    dataset1 = (
        pipeline
        | 'Read Dataset1' >> beam.io.ReadFromText('dataset1.csv')
        | 'Parse Dataset1' >> beam.Map(lambda line: line.split(','))
        | 'Filter ARAP and ACCR' >> beam.Filter(lambda fields: fields[4] in ['ARAP', 'ACCR'])
        | 'Map to KV (counter_party, (rating, value))' >> beam.Map(lambda fields: (fields[2], (int(fields[3]), int(fields[5]))))
    )

    dataset2 = (
        pipeline
        | 'Read Dataset2' >> beam.io.ReadFromText('dataset2.csv')
        | 'Parse Dataset2' >> beam.Map(lambda line: line.split(','))
        | 'Map to KV (counter_party, tier)' >> beam.Map(lambda fields: (fields[0], int(fields[1])))
    )

    # Join dataset1 and dataset2 using CoGroupByKey
    joined_data = (
        {'dataset1': dataset1, 'dataset2': dataset2}
        | 'CoGroupByKey' >> CoGroupByKey()
        | 'Filter Non-Matching Records' >> beam.Filter(lambda (key, values): 'dataset1' in values and 'dataset2' in values)
        | 'Flatten' >> beam.FlatMap(lambda (key, values): [(key, v) for v in values['dataset1']])
    )

    # Define a function to calculate the metrics
    def calculate_metrics(data):
        counter_party, records = data
        max_rating = max(rating for rating, _ in records)
        sum_arap = sum(value for rating, value in records if rating == 'ARAP')
        sum_accr = sum(value for rating, value in records if rating == 'ACCR')
        return (counter_party, (max_rating, sum_arap, sum_accr))

    # Apply the calculate_metrics function
    calculated_data = (
        joined_data
        | 'Calculate Metrics' >> beam.Map(calculate_metrics)
    )

    # Create a new record to add totals for each of legal_entity, counterparty, and tier
    total_records = (
        calculated_data
        | 'Add Total Records' >> beam.Map(lambda data: ('Total', data))
        | 'Group by Tier' >> beam.GroupByKey()
        | 'Calculate Totals' >> beam.Map(lambda (tier, records): (tier, sum(r[0] for r in records), sum(r[1] for r in records), sum(r[2] for r in records)))
        | 'Flatten Total Records' >> beam.FlatMap(lambda (tier, max_rating, sum_arap, sum_accr): [
            ('Total', 'Total', tier, max_rating, sum_arap, sum_accr)
        ])
    )

    # Merge calculated_data and total_records
    merged_data = calculated_data | 'Merge Total Records' >> beam.Flatten(total_records)

    # Write the result to an output file
    merged_data | 'Write Output' >> beam.io.WriteToText('output_beam.txt')
