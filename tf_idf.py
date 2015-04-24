import math


def tfidf(term, doc, corpus):
    return tf(term, doc) * idf(term, corpus)


def idf(term, corpus):
    docs = corpus.split('@en .')[:-1]
    corpus = map(lambda d: d.split('"')[1], docs)
    doc_count = len(corpus)
    docs_with_term = sum(map(lambda d: int(term in d), corpus))
    if docs_with_term:
        return math.log(doc_count / float(docs_with_term))
    return 0


def tf(word, doc):
    return doc.split().count(word) / float(len(doc.split()))
