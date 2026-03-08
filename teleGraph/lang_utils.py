# lang_utils.py
# Author: Bartosz Pankratz <bartosz.pankratz@gmail.com>
# Text cleanining, extraction and prediction scripts; 
# used for working with Telegram message content.   
#
# License: MIT License

import re

def remove_unsupported_characters(text):
    '''Function to remove invalid XML characters from text'''
    valid_xml_chars = (
        "[^\u0009\u000A\u000D\u0020-\uD7FF\uE000-\uFFFD"
        "\U00010000-\U0010FFFF]"
    )
    cleaned_text = re.sub(valid_xml_chars, '', text)
    return cleaned_text
	
def remove_emojis(text):
    '''Remove emojis from a given piece of text (for language classification)'''
    regrex_pattern = re.compile(pattern = "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags = re.UNICODE)
    return regrex_pattern.sub(r'',text)

def extract_mentions(text, post_only = False):
    '''Function that extracts Peer's (channel, user, group) mentions from post and returns Peer name.
        :param str text: content of the post
        :param bool post_only: extract links to the specific posts only (default False)'''
    text_cleaned = "".join(text.splitlines())
    text_cleaned = remove_emojis(text_cleaned)
    if post_only:
        links = re.findall(r'(?<=https:\/\/t.me\/)\w+?(?=\/\d+)', text_cleaned, flags=re.A) #link to post
    else:
        links = re.findall(r'(?<=https:\/\/t.me\/)\w+(?<!\_|\W)', text_cleaned, flags=re.A) #link to post and channel
    mentions = re.findall(r'(?:(?<=^)|(?<=\W)|(?<=\_))(\@\w+)(?<!\_|\W)', text_cleaned, flags=re.A)
    return list(set(map((lambda x: '@' + x if x[0] != '@' else x), mentions + links)))