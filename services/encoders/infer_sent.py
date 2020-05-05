import operator
import os
import spacy
import numpy as np
import torch
from services.encoders.infersent_models import FbInferSent


class InferSent():
    def __init__(self, input_config):
        storage_folder = input_config['modelPath']

        self.config = {
            "modelName": input_config['modelName'],
            "word2VecModelName": input_config['word2VecModelName'],
            "binaryWord2VecModel": False,
            "batchSize": 64,
            "wordEmbDim": 300,
            #for other models without (256) prefix the (encLSTMDim) should be 1024
            "encLSTMDim": input_config['encLSTMDim'],
            "encoderType":  input_config['encoderType'],
            "k_top_words_to_load": 10 ** 5
        }

        self.base_path = storage_folder
        self.nlp = spacy.load('en_core_web_sm')


        self.model_path = os.path.join(self.base_path, self.config.get('modelName'))
        self.word2vec_model_path = os.path.join(self.base_path, self.config.get('word2VecModelName'))
        self.use_cuda = self.config.get('use_cuda', False)
        self.k = self.config.get('KTopWordsToLoad', 10**10)
        self.k =10**5
        self.batch_size = self.config.get('batchSize')
        self.word_emb_dim = self.config.get('wordEmbDim')
        self.enc_lstm_dim = self.config.get('encLSTMDim')
        self.binary_w2vec_model = self.config.get('binaryWord2VecModel')
        self.encoder_type = self.config.get('encoderType', 'LSTM')
        self.params_model = {'bsize': self.batch_size, 'word_emb_dim': self.word_emb_dim, 'enc_lstm_dim': self.enc_lstm_dim,
                        'pool_type': 'max', 'dpout_model': 0.0, 'version': 1, 'encoder_type': self.encoder_type}


        self._load_models()

    def _load_models(self):
        # try:
            self.model = FbInferSent(self.params_model)
            self.model.load_state_dict(torch.load(self.model_path, map_location='cpu'))
            self.model = self.model.cuda() if self.use_cuda else self.model
            self.model.set_w2v_path(self.word2vec_model_path)

            if self.binary_w2vec_model:
                self.model.build_vocab_from_model()
            else:
                self.model.build_vocab_k_words(K=self.k)
        # except Exception as e:
        #     print('errorr loading the model', e)

    def encode(self, text, tokenize = True):
        '''this function take a text or could be generator, and return the text embedding vector'''
        result = self.model.encode([text], tokenize=tokenize)[0]
        encoded = result['encoded']
        missing_tokens = result['missing_tokens']

        return encoded[0]

    def get_model_keywords(self, text):
        text = ' '.join([token.lower_ for token in self.nlp(text) if (not token.is_stop and not token.pos_ == 'NUM' and len(token) > 3)])
        x, y = self.model.visualize(text, tokenize=False)
        result = [(x[i], y[i]) for i in range(len(x))]
        sorted_res = sorted(result, key=lambda tup: tup[1], reverse=True)
        return list(set([word for (word, weight) in sorted_res[:20] if len(word) > 3 and word not in ['<s>', '</s>']]))

