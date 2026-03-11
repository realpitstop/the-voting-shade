# The Voting Shade
Last updated: 03/10/26
## Purpose
Project to collect data on legislator voting and funding, and bill topics. Intention to bring accountability to Congress and ease access to data through abstraction of database interaction.

## Usage
- Easily access data linking donations, bills, votes, and committees
- Utilize parsed tables / ingestion scripts from government sources
- Classify bills into Comparative Agenda Project policy topics + subtopics
- Link Corporate PACs to real companies + industries

![Demonstrating how the pipeline converts a request into SQL into data](text2sql.png)

## Contains
1. Ingestion of data from: FEC, SEC, Congress, Senate, House
2. Parsing of data (Bills, PACs, etc.)
3. Training & Inference for automatic Bill topic Classifier
4. Matching of Corporate PACs to SEC filing companies (60% of total PAC money)
5. Advanced JSON Request --> SQL Query Converter (Faiss-powered column and value matching)

## Recreation steps
1. Make data/ folder with clean/ and raw/ (with annotation/, govinfo/ (billstatus/, bills/), members/, pacs/)
2. Ingest data (make your own headers) (ingest_xxxxx.py)
3. Create dataset + train model using annotation/ folder (NOT VERIFIED TO WORK)
4. Parse data (parse_xxxxx.py), !! (parse_pacs before parse_transactions, parse_bills before parse_votes)

## To do SQL queries
1. Create graph (text2sql/make_graph.py)
2. Type a request into request variable in execute_sql.py and execute file

## Data dates
- Bills, Votes, & Congresspeople: 113th to 119th Congress (2013 to Present (until 2027))
- PAC transactions: 1999-2026

## If you find any errors or data collection inaccuracies, please leave a note somehow.
