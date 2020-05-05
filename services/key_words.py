import numpy as np
import gensim
from rake_nltk import Rake
from gensim.summarization import keywords
import spacy
import operator

nlp = spacy.load('en_core_web_sm')
w2v_path = 'C:\zorba\storage\glove.840B.300d.lower.txt'
r = Rake()
word_vec = {}
k=0
with open(w2v_path, errors='ignore') as f:
    for line in f:
        word, vec = line.split(' ', 1)
        word_vec[word] = np.fromstring(vec, sep=' ')
        k+=1

def keywords_nltk_rake(preprocessed_text, words):
    if isinstance(preprocessed_text, list):
        r.extract_keywords_from_sentences(preprocessed_text)
    else:
        r.extract_keywords_from_text(preprocessed_text)
    keywords = r.get_ranked_phrases_with_scores()
    keywords = keywords[0:words]
    return keywords

def get_semantic_keywords(text):
    matrix = []
    tokens = []
    vistied = {}
    vecs_length = {}
    for token in nlp(text):
        lemma = token.lemma_.lower()
        if not token.is_stop and len(token) > 3 and lemma not in vistied and token.lower_ not in vistied and token.lower_ in word_vec.keys():
            try:
                vec = word_vec[token.lower_]
                matrix.append(np.asarray(vec))
                vecs_length[token.lower_] = np.linalg.norm(np.array(vec) - np.array([0] * 300))
                tokens.append(token.lower_)
                vistied[lemma] = True
                vistied[token.lower_] = True
            except Exception as e:
                print(e)

    matrix = np.asarray(matrix)
    matrix = matrix.mean(axis=0)
    sims = []
    for token in tokens:
        vec = word_vec[token]
        sims.append((token, np.corrcoef(matrix, np.asarray(vec))[1, 0]))

    sorted_semanitc = sorted(sims, key=lambda tup: tup[1], reverse=True)
    sorted_semanitc = [word for (word, weight) in sorted_semanitc]

    sorted_length = sorted(vecs_length.items(), key=operator.itemgetter(1))
    sorted_length = [i[0] for i in sorted_length]
    return sorted_semanitc, sorted_length


keywords_gensim = keywords('hello', words = 10, scores = True, lemmatize = True)