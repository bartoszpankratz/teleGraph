# lang_utils.py
# Author: Bartosz Pankratz <bartosz.pankratz@gmail.com>
# Text cleanining, extraction and prediction scripts; 
# used for working with Telegram message content.   
#
# License: MIT License

import re
from warnings import warn, catch_warnings, simplefilter

try:
    with catch_warnings():
        simplefilter("ignore")
        from huggingface_hub import hf_hub_download
        import fasttext
except ImportError:
    warn('''Language classification model not avalaible; \
        Install: 
        - huggingface-hub 0.33.1
        - fasttext 0.9.2
        - numpy 1.26.4
        or provide other model''',
        category=ImportWarning,
        )
        
        
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
    
def replace_links(text, website_name = True):
    '''Replace all links, except telegram ones: https://t.me/...
        with #link or #website_name token
        default website_name = True'''
    text = remove_emojis(text)
    reg = r'(http|ftp|https):\/\/(www\.|)(?:(?!t.me)([\w_-]+)((?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-]))'
    if website_name:
        return re.sub(reg, '#' +'\\3', text)
    else:
        return re.sub(reg, "#link", text)
        
def replace_usernames(text, peers_dict, with_ids = False):
    '''Replace all links with usernames stored in peers_dict cache.
       If username is not stored in peers_dict, then replace it with '@Username' token.
       If with_ids = True, then @Username is replaced with @user_id
        :param str text: body of text 
        :param dict peers_dict: if provided, dictionary will store peers' info in a format: 
        userrname:{'entity_type':entity_type, 'entity_id':entity_id} (default None)
        :param bool with_ids: replace @username with @user_id (default False)'''
    text = remove_emojis(text)
    mentions = extract_mentions(text)
    if not mentions:
        return text
    link_pattern = re.compile(r'(http|ftp|https):\/\/(www\.|)(t\.me\/)(\w+(?<!\_|\W))(([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])|)',
                              flags = re.UNICODE)
    text = link_pattern.sub('@' +'\\4', text)
    pattern = re.compile("|".join(mentions))
    if with_ids:
        return pattern.sub(lambda x: '@' + str(peers_dict[x.group(0)]['entity_id']) if x.group(0) in peers_dict else "@Username", text)
    else:
        return pattern.sub(lambda x: x.group(0) if x.group(0) in peers_dict else "@Username", text)

def get_fasttext(repo_id="facebook/fasttext-language-identification", filename="model.bin"):
    '''Download language model (default fasttext) from a given repo'''
    try:
        model_path = hf_hub_download(repo_id=repo_id, filename=filename)
        lang_model = fasttext.load_model(model_path)
        return lang_model
    except ImportError:
        warn("Language classification model not avalaible; \
            Install huggingface-hub 0.33.1 and fasttext 0.9.2 or provide other model",
        category=ImportWarning,
        )
        return None

def predict_post_language(post_content, lang_model_predict):
    '''If correct model installed, predict the language of the post.
       If not, return None.
       :param str post_content: body of text
       :param function lang_model_predict: language prediction function'''
    try:
        text_cleaned = "".join(post_content.splitlines())
        text_cleaned = remove_emojis(text_cleaned)
        predlang = lang_model_predict(text_cleaned, k=1)[0][0]
        return predlang
    except ImportError:
        warn("Language classification model not avalaible; \
            Install fasttext 0.9.2 or provide other model",
        category=ImportWarning,
        )
        return None