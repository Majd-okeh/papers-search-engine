import operator
import numpy as np
import gensim
from rake_nltk import Rake
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
from gensim.summarization import keywords
from gensim import corpora, models
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from nltk.stem.porter import *
import nltk
from fuzzywuzzy import fuzz, process
from numbers import Number
import spacy



class KeywordExtractor():
    def __init__(self):
        self.r = Rake()
        self.nlp = spacy.load('en_core_web_sm')
        w2v_path = 'C:\zorba\storage\glove.840B.300d.lower.txt'

        self.word_vec = {}
        k=0
        with open(w2v_path, errors='ignore') as f:
            for line in f:
                word, vec = line.split(' ', 1)
                self.word_vec[word] = np.fromstring(vec, sep=' ')
                k+=1


    def remove_repeated_words(self, words_list):
        result = []
        visited = {}
        text = ' '.join(words_list)
        for token in self.nlp(text):
            if not token.is_stop and len(token) > 3 and token.lemma_ not in visited:
                result.append(token.lower_)
                visited[token.lemma_] = True
        return result



    def keywords_nltk_rake(self, text, words):
        self.r.extract_keywords_from_text(text)
        keywords = self.r.get_ranked_phrases_with_scores()
        keywords = keywords[0:words]
        return keywords



    def combine_keywords(self, list_of_list):
        combined = []
        for list in list_of_list:
            for item in list:
                if isinstance(item, tuple):
                    if isinstance(item[0], Number):
                        word = item[1]
                    else:
                        word = item[0]
                else:
                    word = item

                if combined == []:
                    combined.append(word)
                else:
                    highest = process.extractOne(word, combined)
                    if highest[1] < 90:
                        combined.append(word)
        return combined

    def get_semantic_keywords(self, text):
        matrix = []
        tokens = []
        vistied = {}
        vecs_length = {}
        for token in self.nlp(text):
            if not token.is_stop and len(token) > 3 and token.lemma_ not in vistied and token.lower_ in self.word_vec.keys():
                try:
                    vec = self.word_vec[token.lower_]
                    matrix.append(np.asarray(vec))
                    vecs_length[token.lower_] = np.linalg.norm(np.array(vec) - np.array([0] * 300))
                    tokens.append(token.lower_)
                    vistied[token.lemma_] = True
                except Exception as e:
                    print(e)

        matrix = np.asarray(matrix)
        matrix = matrix.mean(axis=0)
        sims = []
        for token in tokens:
            vec = self.word_vec[token]
            sims.append((token, np.corrcoef(matrix, np.asarray(vec))[1, 0]))

        sorted_semanitc = sorted(sims, key=lambda tup: tup[1], reverse=True)
        sorted_semanitc = [word for (word, weight) in sorted_semanitc]

        sorted_length = sorted(vecs_length.items(), key=operator.itemgetter(1))
        sorted_length = [i[0] for i in sorted_length]
        return sorted_semanitc, sorted_length

    def get_keywords(self, text):
        text_in = text.casefold()

        try:
            # Generate gensim keywords:
            keywords_gensim = keywords(text_in, words=3, scores=True)
            # Generate nltk-rake keywords
            keywords_rake = self.keywords_nltk_rake(text_in, words = 3)
        except:
            keywords_gensim = keywords(text_in, words=2, scores=True)
            keywords_rake = self.keywords_nltk_rake(text_in, words = 2)

        #semanticly generated keywords
        semantic_keywords, length_keywords = self.get_semantic_keywords(text_in)

        # combine lists
        combined_list = self.combine_keywords([keywords_gensim, keywords_rake])
        combined_list = self.combine_keywords([combined_list[:5], semantic_keywords[:4], length_keywords[-5:]])

        return self.remove_repeated_words(combined_list)


# from services.cassandra_ import CassandraDatabase
# summary_repo = CassandraDatabase(project_name='papers', repo_name='summary', id_sql_type='BIGINT',
#                                      content_sql_type="TEXT")
# text = summary_repo.read(1)[0]
# e = KeywordExtractor()
# print(e.get_keywords(text))
