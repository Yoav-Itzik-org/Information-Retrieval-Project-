# Constants
PROJECT_ID = "ir-assignment3-413720"
BUCKET_NAME = "ir208752667"
PICKLES_FOLDER = "global_pickles"
STOP_WORDS_FILE_NAME = "stop_words"
SEGMENTS_WEIGHTS_DICT = {"title": 0.7, "body": 0.23, "anchor": 0.07}
ID_TITLE_DICT = "title"; ID_DICTS = [ID_TITLE_DICT, "page_rank", "page_views"]
LOCAL_CREDENTIALS_JSON_PATH = r"C:\Users\t-yzelinger\OneDrive - post.bgu.ac.il\שנה ג'\סמסטר ה'\אחזור מידע\מטלות\מטלה מעשית 3\ir-assignment3-413720-d6c7c7c7a981.json"
PRINT_LOG = False

import os
from google.cloud.storage import Client
from google.auth.exceptions import DefaultCredentialsError
from collections import Counter, defaultdict
from re import compile, MULTILINE, IGNORECASE, VERBOSE, UNICODE
import pickle
from nltk.stem.porter import PorterStemmer
from math import log
from threading import Thread
from heapq import heappop, heappush, heapify

def print_log(*text):
    if PRINT_LOG:
        print()
        if len(text) > 0:
            import time
            print(f"[{time.strftime('%H:%M:%S')}]:", *text)

# Managing the bucket access
class BucketManager:
    _bucket_object = None

    @staticmethod
    def get_bucket():
        print_log("Getting the bucket")
        if BucketManager._bucket_object is None:
            # Get the Google's credentials
            try:
                google_client = Client()
            except DefaultCredentialsError as credentials_error:
                if not os.path.exists(LOCAL_CREDENTIALS_JSON_PATH):
                    raise credentials_error
                print_log("Getting the bucket using local credentials")
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = LOCAL_CREDENTIALS_JSON_PATH
                google_client = Client()
            finally:
                BucketManager._bucket_object = google_client.bucket(BUCKET_NAME)
        
    @staticmethod
    def start():
        BucketManager.get_bucket()

    @staticmethod
    def get_full_path(file_name):
        return f"{PICKLES_FOLDER}/{file_name.lower()}.pkl"

    @staticmethod
    def load_pickle(file_name: str):
        print_log(f"Downloading {file_name} from the bucket")
        absolute_path = BucketManager.get_full_path(file_name)
        bucket_blob = BucketManager._bucket_object.blob(absolute_path)
        try:
            pickle_content = bucket_blob.download_as_bytes()
            return pickle.loads(pickle_content)
        except Exception as e:
            raise e # TODO - THINK IF NECESSARY TO REMOVE
            # return None

    @staticmethod
    def store_pickle(file_name: str, object_to_store):
        print_log(f"Uploading {file_name} to the bucket")
        absolute_path = BucketManager.get_full_path(file_name)
        with BucketManager._bucket_object.blob(absolute_path).open("wb") as pickle_file:
            pickle.dump(object_to_store, pickle_file)

# Managing the inverted indicies
class InvertedIndexManager:
    indicies_dict = None

    # Get a the inverted indicies from the bucket
    @staticmethod
    def __retrieve_inverted_indicies():
        indicies_dict = {}
        for index_name in SEGMENTS_WEIGHTS_DICT:
            pickle_name = index_name + "_inverted_index"
            indicies_dict[index_name] = BucketManager.load_pickle(pickle_name)
        return indicies_dict

    @staticmethod
    def start():
        InvertedIndexManager.indicies_dict = InvertedIndexManager.__retrieve_inverted_indicies()

    # Load a posting list
    @staticmethod
    def get_posting_list(inverted_index_name, term):
        return InvertedIndexManager.indicies_dict[
            inverted_index_name
        ].read_a_posting_list(inverted_index_name, term, BUCKET_NAME)

class PageManager:
    id_dict_dicts = {}
    
    @staticmethod
    def start():
        for id_dict in ID_DICTS:
            PageManager.id_dict_dicts[id_dict] = BucketManager.load_pickle(id_dict + "_dict")
            if PageManager.id_dict_dicts[id_dict] is None:
                raise FileNotFoundError(f"{id_dict}'S PICKLE FILE WAS NOT FOUND IN THE BUCKET")
    
    @staticmethod
    def get_page_item(id, dict_name):
        dict_name = dict_name.lower()
        if dict_name not in ID_DICTS:
            raise FileExistsError(f"{dict_name} IS NOT A VALID ID DICT NAME")
        id_dict = PageManager.id_dict_dicts[dict_name]
        return id_dict.get(id, 1 if dict_name != "title" else "ERROR - TITLE NOT FOUND")
    
    @staticmethod
    def get_number_of_pages():
        return len(PageManager.id_dict_dicts[ID_TITLE_DICT])

# Tokenizing text
class Tokenizer:
    # Stemmer
    __stemmer = PorterStemmer()

    # Tokenizing Patterns
    # @staticmethod
    # def __get_html_pattern():
    #     return r"<[^>]+>"

    # @staticmethod
    # def __get_date_pattern():
    #     years = r"\d{4}"
    #     regex29 = r"(?:\d|[12]\d)"
    #     ws = r"\s"
    #     allMonthsFull = [
    #         "Jan",
    #         "Feb",
    #         "Mar",
    #         "Apr",
    #         "May",
    #         "Jun",
    #         "Jul",
    #         "Aug",
    #         "Sep",
    #         "Oct",
    #         "Nov",
    #         "Dec",
    #         "January",
    #         "February",
    #         "March",
    #         "April",
    #         "May",
    #         "June",
    #         "July",
    #         "August",
    #         "September",
    #         "October",
    #         "November",
    #         "December",
    #     ]
    #     nonFeb = lambda months: (
    #         month for month in months if not month.startswith("Feb")
    #     )
    #     monthsWith31 = lambda months: (
    #         month
    #         for i, month in enumerate(months, 1)
    #         if i % 12 in (1, 3, 5, 7, 8, 10, 12)
    #     )
    #     mmdd = f"(?:(?:{'|'.join(allMonthsFull)}){ws}{regex29}|(?:{'|'.join(nonFeb(allMonthsFull))}){ws}(?:30)|(?:{'|'.join(monthsWith31(allMonthsFull))}){ws}(?:31)),"
    #     ddmm = f"(?:{regex29}{ws}(?:{'|'.join(allMonthsFull)})|(?:30){ws}(?:{'|'.join(nonFeb(allMonthsFull))})|(?:31){ws}(?:{'|'.join(monthsWith31(allMonthsFull))}))"
    #     return f"(?:{mmdd}|{ddmm}){ws}{years}"

    # @staticmethod
    # def __get_time_pattern():
    #     hh12 = r"(?:0\d|1[0-2])"
    #     mm60 = r"(?:[0-5]\d|60)"
    #     h24 = r"(?:\d|1\d|2[0-3])"
    #     m60 = r"(?:\d|[1-5]\d|60)"
    #     return f"{hh12}\.{mm60}(?:A|P)M|{hh12}{mm60}(?:a|p)\.m\.|{h24}:{m60}:{m60}$"

    # @staticmethod
    # def __get_number_pattern():
    #     lookBehind = r"(?<![\-+0-9.,a-z])"
    #     leadingNumber = r"[+-]?[1-9]\d{0,2}"
    #     trio = r"(?:,\d{3})"
    #     decimalOption = r"(?:\.(?:\d+))?"
    #     lookAhead = r"(?:(?!(?:\w|[a-z]))(?![,.]\w))"
    #     return f"{lookBehind}(?:(?:{leadingNumber}(?:{trio}*)|0){decimalOption}){lookAhead}"

    # __percent_pattern = __get_number_pattern() + "%"

    # @staticmethod
    # def __get_word_pattern():
    #     alpha = r"[A-Za-z]"
    #     validChar = f"(?:{alpha}|')"
    #     word = f"{alpha}{validChar}*"
    #     return f"(?<!-){word}(?:(?:-{word})*)"

    # Parser
#     __RE_WORD = compile(
#         rf"""
# (
#     # parsing html tags
#      (?P<HTMLTAG>{__get_html_pattern()})
#     # dates
#     |(?P<DATE>{__get_date_pattern()})
#     # time
#     |(?P<TIME>{__get_time_pattern()})
#     # Percents
#     |(?P<PERCENT>{__percent_pattern})
#     # Numbers
#     |(?P<NUMBER>{__get_number_pattern()})
#     # Words
#     |(?P<WORD>{__get_word_pattern()})
#     # space
#     |(?P<SPACE>[\s\t\n]+)
#     # everything else
#     |(?P<OTHER>.))""",
#         MULTILINE | IGNORECASE | VERBOSE | UNICODE,
#     )
    __RE_WORD = compile(r"""[\#\@\w](['\-]?\w){2,24}""", UNICODE)

    # Stopwords
    all_stop_words = None

    @staticmethod
    def store_stop_words(words=[]):
        global STOP_WORDS_FILE_NAME
        current_stop_words_set = BucketManager.load_pickle(STOP_WORDS_FILE_NAME)
        if current_stop_words_set is None:
            from nltk.corpus import stopwords
            from nltk import download as nltk_download
            nltk_download("stopwords")
            current_stop_words_set = frozenset(stopwords.words("english"))
        updated_stop_words_set = current_stop_words_set.union(words)
        Tokenizer.all_stop_words = updated_stop_words_set
        BucketManager.store_pickle(STOP_WORDS_FILE_NAME, updated_stop_words_set)

    @staticmethod
    def load_stop_words():
        global STOP_WORDS_FILE_NAME
        Tokenizer.all_stop_words = BucketManager.load_pickle(STOP_WORDS_FILE_NAME)
        if Tokenizer.all_stop_words is None:
            print_log("Stop words not found")
            # Tokenizer.store_stop_words()

    @staticmethod
    def start():
        Tokenizer.load_stop_words()
    
    @staticmethod
    def include_token(token, all_stop_words):
        return not (token in all_stop_words or token.isspace())

    @staticmethod
    def tokenize(text, all_stop_words=None):
        all_stop_words = Tokenizer.all_stop_words if all_stop_words is None else all_stop_words
        tokens = map(lambda token: token.group(), Tokenizer.__RE_WORD.finditer(text.lower()))
        tokens = filter(lambda token: Tokenizer.include_token(token, all_stop_words), tokens)
        tokens = map(lambda token: Tokenizer.__stemmer.stem(token), tokens)
        return Counter(tokens)

# Running the search
class BackendSearch:
    
    N = None
    
    @staticmethod
    def start():
        BackendSearch.N = PageManager.get_number_of_pages()

    @staticmethod
    def process_query(query):
        entries_counter = Tokenizer.tokenize(query)
        return entries_counter.items()
    
    def segment_search(self, query_tokens_pairs, segment_name):
        docs_ratings = defaultdict(int)
        bool_dict = defaultdict(bool)
        first_run = True
        for query_token, query_tf in query_tokens_pairs:
            new_bool_dict = defaultdict(bool)
            token_posting_list = InvertedIndexManager.get_posting_list(
                segment_name, query_token
            )
            token_df = len(token_posting_list)
            if token_df == 0:
                continue
            token_idf = log(BackendSearch.N + 1 / token_df, 10)
            query_token_tfidf = query_tf * token_idf
            for doc_id, doc_term_w in token_posting_list:
                docs_ratings[doc_id] += doc_term_w * query_token_tfidf
                new_bool_dict[doc_id] = first_run or bool_dict[doc_id]
            bool_dict = new_bool_dict
            first_run = False
        if segment_name == "body" and sum(bool_dict.values()) >= 30:
            docs_ratings = {doc_id: docs_ratings[doc_id] * SEGMENTS_WEIGHTS_DICT[segment_name] for doc_id in bool_dict if bool_dict[doc_id]}
        self.segments_results[segment_name] = docs_ratings
        print_log("finished segment: ", segment_name)
    
    def search(self, query):
        query_tokens_pairs = BackendSearch.process_query(query)
        segment_threads = []
        for segment_name in SEGMENTS_WEIGHTS_DICT:
            segment_threads.append(Thread(target=self.segment_search, args=(query_tokens_pairs, segment_name)))
            segment_threads[-1].start()
        for segment_thread in segment_threads:
            segment_thread.join()
        chosen_dict = "body"
        if len(self.segments_results[chosen_dict]) < 30:
            chosen_dict = "title"
            if len(self.segments_results[chosen_dict]) < 30:
                chosen_dict = "anchor"
        docs_ratings = self.segments_results[chosen_dict]
        # heap = []
        # heapify(heap)
        # for doc_id in docs_ratings:
        #     for segment_name in SEGMENTS_WEIGHTS_DICT:
        #         if segment_name == chosen_dict:
        #             continue
        #         segment_doc_tf = self.segments_results[segment_name][doc_id]
        #         docs_ratings[doc_id] += (
        #             segment_doc_tf * SEGMENTS_WEIGHTS_DICT[segment_name]
        #         )
        #     docs_ratings[doc_id] += log(PageManager.get_page_item(doc_id, "page_rank"), 10) + log(PageManager.get_page_item(doc_id, "page_views"), 20)
        #     heappush(heap, (-1 * docs_ratings[doc_id], doc_id))
        # docs_sorted = []
        # for _ in range(30):
        #     _, doc_id = heappop(heap)
        #     doc_title = PageManager.get_page_item(doc_id, "title")
        #     docs_sorted.append((str(doc_id), doc_title))
        # return docs_sorted
        doc_rating = lambda doc_id: sum(self.segments_results[segment_name][doc_id] * SEGMENTS_WEIGHTS_DICT[segment_name] for segment_name in SEGMENTS_WEIGHTS_DICT) + log(PageManager.get_page_item(doc_id, "page_rank"), 10) + log(PageManager.get_page_item(doc_id, "page_views"), 20)
        doc_dict = {(str(doc_id), PageManager.get_page_item(doc_id, "title")): doc_rating(doc_id) for doc_id in docs_ratings}
        return sorted(doc_dict, key= doc_dict.get, reverse=True)[: min(30, len(doc_dict))]
        # ratings = sorted(ratings, key=lambda doc_rating: doc_rating[1], reverse=True)[: min(30, len(docs_ratings))]
        # return list(map(lambda doc_rating: (str(doc_rating[0]), PageManager.get_page_item(doc_rating[0], "title")), ratings))
        
    
    def __init__(self):
        self.segments_results = {segment_name: None for segment_name in SEGMENTS_WEIGHTS_DICT}

def system_start():
    BucketManager.start()
    print_log("Retriveing pickles from bucket")
    InvertedIndexManager.start()
    Tokenizer.start()
    PageManager.start()
    BackendSearch.start()

system_start()
print(BackendSearch().search("Apple orange banana"))
