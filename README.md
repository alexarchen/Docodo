# DOCODO 
Documental full-text search engine server & library written on C#

Copyright (c) Alexey A. Zakharchenko
GNU GPL 3

# Features!

  - Fast indexing from different sources: files, http, DB
  - Search with vocabularies and stemmators
  - Morphological and "exact" search
  - Console, link, RESTful interface

# Using 
## As Console app
Run application and follow instuctions
You can create index and search from console

## As REST server
Run application 
```sh
docodo [-p:<port>]
```
and send search request
```sh
127.0.0.1:<port>/search?req=<request>[&params]
```
and receive pure JSON

## Us .NET library

Install DOCODO.NET package

