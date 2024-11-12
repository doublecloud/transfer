# Clickhouse Source

Support 2 mode:

1. Hetero mode: read data to other storage, read via native protocol
2. Homo mode: read data to other clickhouse cluster, data would be readed blindly, by default as csv row, and streamed as batches to target db. Homo mode a lot more effective, since it not parse any data
