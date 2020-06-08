import nltk
from nltk.corpus import stopwords


def get_stopwords(lang):
    nltk.download("stopwords")

    stop_word_keys = {
        "ar": "arabic",
        "da": "danish",
        "de": "german",
        "en": "english",
        "es": "spanish",
        "fi": "finnish",
        "fr": "french",
        "hu": "hungarian",
        "id": "indonesian",
        "it": "italian",
        "nl": "dutch",
        "no": "norwegian",
        "pt": "portuguese",
        "ro": "romanian",
        "ru": "russian",
        "sv": "swedish"
    }

    if lang in stop_word_keys:
        stop_words = set(stopwords.words(stop_word_keys[lang]))
    else:
        stop_words = set()

    return stop_words
