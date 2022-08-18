# dbtrain-lab

## Summary

1. executable:

    - clear.cc: clear all databases
    - init.cc: init a database
    - shell.cc: a simple SQL shell

2. src: source code

3. third_party: antlr4 for parser

## Modules
0.  minios, system, backend, excetable, parser
- The runtime environment.
- Provided by the teacher.

1. exception

2. utils
- Bitmap and PrintTable

3. page
- API for MiniOS's memory page.
- config in macros.h
- Important API
  - Page::SetData, Page::GetData, Page::SetHeader, Page::GetHeader
  - LinkedPage::PushBack, LinkedPage::PopBack
  - RecordPage

4. field

- type in [ Int, Float, String ]

5. record
- FixedRecord and VariableRecord

6. condition
- conditional search

7. table
- Important API
  - Table::GetRecord
  - Table::InsertRecord
  - Table::DeleteRecord
  - Table::UpdateRecord
  - Table::SearchRecord
  - Table::NextNotFull

8. manager

- TableManager, IndexManager and TransactionManager

9.  result

- connect parser module and database kernel
