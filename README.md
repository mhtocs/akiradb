# AkiraDB

Search is overated


AkiraDB is a tiny search engine, a sort of alternative to elasticsearch.

The main goal of this project is to be able to store & search machine generated logs 
type data. 

A general use of elasticsearch is store machine generated logs like data, but i
think elasticsearch is a overkill for this, and it requires too much resources
to handle small set of dataset. for logs search usability instant search is not 
really needed and most users mostly want that their machine should be able to 
search through years worth of data without blowing their budget.

Also Elasticsearch is not really designed for type of search that is generally done on machine generated logs.
Elasticsearch's main use case is to do full text search, whereas machine generated logs need a different kind
of search & full text search really doesnt make any sense for it. My goal with this is to have a combination 
of what loki does but with indexing.
