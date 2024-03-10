# Information-Retrieval-Project

## Our goal: Design and implement a search engine that can efficiently and effectively retrieve relevant Wikipedia English articles based on user queries.
### The project is designed to work with a GCP bucket (Credentials are located at the root folder of the repository).
### Using an inverted index structures, the search engine will find the most relevant documents based on the tokens corresponding to the queries.
### Our search engine perform similiary based on Consine Similarity function with an weight of tokens including tfidf.

## Project structure:

### General:
  Report.pdf - report describing the whole workflow of the project.
  indexes_file.txt - specify the whole bucket content.
  startup_script_gcp.sh - startup script to create a vm machine that will run the search engine

### Backend:
#### inverted_index_gcp.py - holds the InvertedIndex class and classes that holds the io logics to the bucket.
#### backend_helper.py - holds helper classes for most of the logic behind the search operations. the classes are:
##### BucketManager
###### Interacts with the bucket. It's key methods are:
####### 1. get_bucket() - loads a bucket instance to the memory.
######## * The next two methods will use the global_pickles folder specifically in the bucket.
####### 2. load_pickle(filename) - given a pickle file name, return the object which it holds.
####### 3. store_picle(file_name, object_to_store) - given a file_name and an objcet - store the object inside a pickle with the given name.
##### InvertedIndexManager
###### Interact with the inverted index objects. It's key methods are:
####### 1.  __retrieve_inverted_indicies() - loads to memory (from the bucket) the inverted index objects.
####### 2. get_posting_list(inverted_index_name, term) - retrieved the posting list of the term at the relevant inverted index object.
##### PageManager
###### interacts with the global items of the pages. Its key methods are:
####### 1. get_page_item(id, dict_name) - return an item of the given id (doc id).
####### 2. get_number_of_pages() - return the number of pages in the corpus.
##### Tokenizer
###### Manage the whole tokenizing process. It's key methods are:
####### 1. load_stop_words() - loads to memory the stop words of the corpus.
####### 2. store_stop_words(words) - adds stop_words to the current set of them.
####### 3. tokenize(text, all_stop_words) - gets a text (and an option the add a stop_words to filter from, if not given it takes from the stored ones) perform tokenize to the text - return counter of the unique words.
##### BackendSearch
###### Wraps the whole search operation. Given a text return the relevant documents. It's key methods are:
####### 1. process query(query) - convert a query to list of tokens (tokenize the query).
####### 2. segment_search(query_tokens_pairs, segment_name) - return all the relevant documents to the query tokens in a relevant segment (title/body/anchor).
####### 3. search(query) - combine it all: gets a query and returns the most relevant documents (with their titles) of the words by all the measuers: title, body, anchor, document pagerank and document pageviews.
### Frontend:
#### run_frontend_in_colab.ipynb - Intstructions to run the frontend in a google colab notebook.
#### run_frontend_in_gcp.sh - commands to add to the gcp vm machine to run the search engine.
#### search_frontend.py - script with a search function to ruturn through a server (by json file) the relevant documents to a query.
