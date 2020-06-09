import spacy

def remove_repeated_words(words_list):
    result = []
    visited = {}
    text = ' '.join(words_list)
    nlp = spacy.load('en_core_web_sm')
    for token in nlp(text):
        if not token.is_stop and len(token) > 3 and token.lemma_ not in visited:
            result.append(token.lower_)
            visited[token.lemma_] = True
    return result

x= ['user', 'users']
print(remove_repeated_words(x))