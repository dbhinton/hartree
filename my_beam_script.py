import apache_beam as beam

# Read the input datasets
dataset1_rows = beam.io.ReadFromText('dataset1.csv')
dataset2_rows = beam.io.ReadFromText('dataset2.csv')

# Transform the dataset1 records into a dictionary format
dataset1_dict = dataset1_rows | 'ToDict' >> beam.Map(lambda row: {
    'invoice_id': row[0],
    'legal_entity': row[1],
    'counter_party': row[2],
    'rating': row[3],
    'status': row[4],
    'value': row[5]
})

# Join the dataset1 dictionary with dataset2 using the 'counter_party' field
joined_dataset = dataset1_dict | 'Join' >> beam.Map(lambda record: (
    record['legal_entity'], record['counter_party'], record['tier'], record['rating'], record['status'], record['value']),
    key=lambda record: (record['legal_entity'], record['counter_party']))

# Calculate the required metrics for each record
calculated_metrics = joined_dataset | 'CalculateMetrics' >> beam.Map(
    lambda record: {
        'legal_entity': record[0],
        'counterparty': record[1],
        'tier': record[2],
        'maximum_rating': max(record[4]),
        'arap_total': sum(record[5]) if record[3] == 'ARAP' else 0,
        'accr_total': sum(record[5]) if record[3] == 'ACCR' else 0
    })

# Calculate the totals for each of legal entity, counterparty, and tier
totals = calculated_metrics | 'CalculateTotals' >> beam.GroupByKey() | 'CalculateTotalMetrics' >> beam.Map(
    lambda record: {
        'legal_entity': record[0][0],
        'counterparty': record[0][1],
        'tier': record[0][2],
        'maximum_rating': max(record[1]),
        'arap_total': sum(record[2]),
        'accr_total': sum(record[3])
    })

# Write the results to the output file
beam.io.Write('output_beam.csv', calculated_metrics, file_naming='output_beam.csv')
beam.io.Write('output_beam.csv', totals, append=True, file_naming='output_beam.csv')
