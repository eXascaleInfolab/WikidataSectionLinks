import string


def process_string(s, stop_words):
    tokens = s.lower().split(" ")
    tokens = [token.strip(string.punctuation) for token in tokens if token not in stop_words
              and token != " "
              and token.strip() not in string.punctuation]

    return tokens


def generate_key(stop_words, *strings):
    tokens = []

    for s in strings:
        s_tokens = process_string(s, stop_words)

        # if either wiki title or section is completely reduced after processing, we discard such keys
        if len(s_tokens) == 0:
            return ""

        tokens.extend(s_tokens)

    return " ".join(sorted(tokens))
