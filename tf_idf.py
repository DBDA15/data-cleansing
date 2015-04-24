import math


def tfidf(term, doc, corpus):
    return tf(term, doc) * idf(term, corpus)


def idf(term, corpus):
    corpus = corpus.map(lambda line:  line.split('"')[1] if line != u'# started 2014-08-21T14:51:59Z' else '')
    doc_count = corpus.count()
    docs_with_term = corpus.map(lambda d: int(term in d)).sum()
    if docs_with_term:
        return math.log(doc_count / float(docs_with_term))
    return 0


def tf(word, doc):
    return doc.split().count(word) / float(len(doc.split()))
