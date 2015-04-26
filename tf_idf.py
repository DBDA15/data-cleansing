import pdb
import math
from collections import Counter


def get_term_dict(corpus):
    #FIXME do not split at " as it kicks out \"aTermInAbstract\"
    docs = corpus.map(lambda x: x.split('"')[1].lower())
    terms = docs.flatMap(lambda x: x.split())
    # collect() forces evaluation of action to be able to broadcast term_dict
    return terms.distinct().collect()


def get_all_docs(corpus):
    return corpus.map(lambda line:  line.split('"')[1]
                      if line != u'# started 2014-08-21T14:51:59Z'
                      else '')


def calc_tfidf(sc, rdd):
    # corpus = sc.textFile(path)
    corpus = rdd
    docs = get_all_docs(corpus)
    # docs.cache()
    doc_count = docs.count()
    term_dict = get_term_dict(corpus)
    # term_dict.cache()
    dwt = n_docs_with_term(docs, term_dict)
    pdb.set_trace()
    return docs.foreach(lambda d: iterate_docs(d, doc_count, term_dict))


def n_docs_with_term(docs, term_dict):
    return docs.map(lambda d: map(lambda t: int(t in d), term_dict))


def iterate_docs(doc, doc_count, term_dict):
    words_in_doc = doc.split().count()
    vector = []
    return term_dict.foreach(lambda term:
                             iterate_terms(doc, term, doc_count, term_dict))


def iterate_terms(doc, term, term_dict, doc_count):
    tf = calc_tf(term, doc)
    idf = calc_idf(docs, doc_count, term_dict)
    return tf * idf


def calc_idf(docs, doc_count, term_dict):
    n = n_docs_with_term(docs, term_dict)
    if n:
        return math.log(doc_count / float(n))
    return 0


def calc_tf(word, doc):
    return doc.split().count(word) / float(len(doc.split()))
