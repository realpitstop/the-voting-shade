# the-voting-shade
03/10/26
## Purpose
Project to collect data on legislator voting and funding, and bill topics. Intention to bring accountability to Congress and ease access to data through abstraction of database interaction.

## Usage


## Contains
1. Ingestion of data from: FEC, SEC, Congress, Senate, House
2. Parsing of data (Bills, PACs, etc.)
3. Training & Inference for automatic Bill topic Classifier
4. Matching of Corporate PACs to SEC filing companies
5. Advanced JSON Request --> SQL Query Converter

## Recreation steps
1. Make data/ folder with clean/ and raw/ (with annotation/, govinfo/ (billstatus/, bills/), members/, pacs/)
2. Ingest data (ingest_xxxxx.py)
3. Create dataset + train model using annotation/ folder (NOT VERIFIED TO WORK, PLEASE LET ME KNOW)
4. Parse data (parse_xxxxx.py), !! (parse_pacs before parse_transactions, parse_bills before parse_votes)

## To do SQL queries
1. Create graph (text2sql/make_graph.py)
2. Type a request into request variable in execute_sql.py and execute file

## If you find any errors or data collection inaccuracies, please leave a note somewhere.
