# data_utils.py
# Author: Bartosz Pankratz <bartosz.pankratz@gmail.com>
# Data preprocessing; 
# Exctract all users info from scraped data, map user_id to @username,
# replace mentions, and anonymize data.   
#
# License: MIT License

import os
import time
import logging
import warnings
import pandas as pd
from collections import defaultdict

logger = logging.getLogger(__name__)

def extract_peers_from_cols(df, id_col, name_col, type_col, peers_dict, variant = 'general'):
    '''Extract Peer's (channel, user, chat) and cache them as a dictionary in a format: 
       userrname:{'entity_type':entity_type, 'entity_id':entity_id} 
        :param pandas.DataFrame df: data frame with scraped Telegram channels
        :param str id_col: column with peers' ID
        :param str name_col: column with peers' Username
        :param str type_col: column with peers' Type
        :param dict peers_dict: dictionary for caching peers
        :param str variant {'mentions', 'reactions', 'general'}:
            Determines the type of columns peers are extracted from.
            - 'general' (default): used for extracting post authors, post forwards and replies
            - 'reactions': used for extracting peers from reactions 
            - 'mentions': used for extracting peers from mentions 
    '''
    if variant not in {'mentions', 'reactions', 'general'}:
        raise ValueError(f"variant '{variant}' not understood")
    filtered_df = df[df[name_col].isna() == False][[id_col, name_col, type_col]].drop_duplicates()
    if variant == "mentions" or variant == "reactions":
        filtered_df = filtered_df[[id_col, name_col, type_col]].map(lambda x: str(x).rstrip().split(' '))
        filtered_df = filtered_df.explode([id_col, name_col, type_col])
        filtered_df = filtered_df[filtered_df[name_col] != '<NA>'].drop_duplicates()  
    if variant == 'mentions':
        #add peers without @username:user_id matching to special key in peers_dict:
        if 'unused peers' not in peers_dict:
            peers_dict['unused peers'] = set()
        peers_dict['unused peers'].update(set(filtered_df[filtered_df[id_col] == '<NA>'][name_col]))
        #filter out peers without @username:user_id matching:
        filtered_df = filtered_df[filtered_df[id_col] != '<NA>']
    #add peers to peers_dict:
    filtered_df[id_col] = filtered_df[id_col].astype('float64').astype('Int64')
    filtered_df = filtered_df.rename(columns={id_col: "entity_id", type_col: "entity_type"})
    peers_dict.update(filtered_df.set_index(name_col).to_dict(orient = 'index'))
    
def extract_peers_from_df(df, peers_dict):
    '''Function extracts Peer's (channel, user, group) and caches them as a dictionary in a format: 
       userrname:{'entity_type':entity_type, 'entity_id':entity_id} 
        :param pandas.DataFrame df: data frame with scraped Telegram channels
        :param dict peers_dict: dictionary for caching peers'''
    #extract authors:
    extract_peers_from_cols(df, 'Author ID', 'Author Username', 'Author Type', peers_dict)
    #extract replies:
    extract_peers_from_cols(df, 'Reply to Author ID', 'Reply to Username', 'Reply to Author Type', peers_dict)
    #extract forwards:
    extract_peers_from_cols(df, 'Forwarded from Author ID', 'Forwarded from Author Username', 'Forwarded from Author Type', peers_dict)
    #extract reactions:
    extract_peers_from_cols(df, 'Reactions IDs', 'Reactions Peer Username', 'Reactions Peer Type', peers_dict, variant = "reactions")
    #extract mentions:
    extract_peers_from_cols(df, 'Mentions IDs', 'Mentions Peer Username', 'Mentions Peer Type', peers_dict, variant = "mentions")

def extract_peers_from_dir(path, filenames):
    '''Function extracts Peer's (channel, user, group) and caches them as a dictionary in a format: 
       userrname:{'entity_type':entity_type, 'entity_id':entity_id} 
        :param str path: path where scraped data is stored 
        :param str filenames: compiled regex for valid filenames in directory'''
    peers = dict()
    for fname in os.listdir(path):
        #extract name and id from valid files:
        matched = filenames.search(fname)
        if not matched:
            continue
        file_extension = fname.rsplit('.', 1)[-1]
        channel, channel_id = matched.group().rsplit('_', 1) #split string into channel name and channel id
        #open file:
        if file_extension == 'xlsx':
           df = pd.read_excel(path + fname, engine='openpyxl')
        elif file_extension == 'parquet':
            df = pd.read_parquet(path + fname, engine='pyarrow')
        elif file_extension == 'csv':
            df = pd.read_csv(path + fname, sep = ';', encoding = 'utf=8')
        else: 
            raise ValueError("Wrong file format! Files can be stored as xlsx, parquet or csv files.")
        if df.empty:
            print(f'{fname} file is empty!')
            logger.warning(f'{fname} file is empty!')
            continue 
        extract_peers_from_df(df, peers)
    if 'unused peers' in peers:
        peers['unused peers'] = {peer for peer in peers['unused peers'] if not peer in peers}
    return peers
    
def id_gen():
    ''' Generate a new ID for peer '''
    counter = 0
    while True:
        cnt = str(counter)
        clock = str(time.time()).replace('.','')[-4:].lstrip('0')
        new_id =  clock + '0' * (10 - len(clock) - len(cnt)) + cnt
        yield int(new_id)
        counter += 1 
        counter = counter % 999999

def generate_new_ids(peers_dict, variant = 'valid_only'):
    '''For peers cached in a format: 
       userrname:{'entity_type':entity_type, 'entity_id':entity_id}, 
       where peers without a matching ID are stored in a key 'unused peers':
       unused peers: {"@Username1", '@Userrname2',...},
       generate new 10-digit IDs and return a new dictionary. 
        :param dict peers_dict: dictionary with cached peers
        :param str variant {'valid_only', 'all', 'unused_only'}:
            Determines the users for whom a new ID will be generated.
            - 'valid_only' (default): generate new IDs only 
               for users already having one (safest variant).
            - 'all': generate new IDs for everyone, including unused peers
            - 'unused_only': generate new IDs only for unused peers 
            Variants 'all' and 'unused_only' might cause trouble. Some users
            have no @Username assigned, so after generating an ID for a @Username
            from the 'unused peers', they might be counted twice in the dataset. 
            To distinguish them from users with valid IDs, their ID will have 
            an '11' prefix added. 
    '''
    if variant not in {'valid_only', 'all', 'unused_only'}:
        raise ValueError(f"variant '{variant}' not understood")
    gen = id_gen()
    if variant == 'valid_only' or variant == 'all':
        new_peers = {key:{'entity_id':next(gen), 'entity_type':val['entity_type']} for (key, val) in peers_dict.items() if key != 'unused peers'}
    else:
        new_peers = peers_dict.copy() 
    if variant == 'unused_only' or variant == 'all':
        if 'unused peers' not in peers_dict:
            warnings.warn("No unused peers in the dictionary, cannot assing IDs")
            return new_peers
        new_peers.pop('unused peers', None)
        new_peers.update((key, {'entity_id': int('11' + str(next(gen))), 'entity_type': "Unknown"})
                        for key in peers_dict['unused peers'] if key not in peers_dict)
    return new_peers
    
def merge_duplicate_peers(peers_dict):
    '''Function takes peers_dict dictionary that caches all users in a format: 
       userrname:{'entity_type':entity_type, 'entity_id':entity_id} 
       and find usernames with the same 'entity_id' (usually channels and respective bots).
       Then, values are merged into a single record and returned as a dictionary:
       entity_id:{'entity_type':entity_type, 'usernames':set(usernames)} 
    '''
    inv_peers = defaultdict(lambda:  {"entity_type": 'User', "usernames": set()})
    for key, val in peers_dict.items():
        if key == 'unused peers':
            continue
        inv_peers[val['entity_id']]['usernames'].add(key) #add name to usernames
        #update entity type, hierarchy is as follows: 1. Channel, 2. Chat, 3. User
        if val['entity_type'] != 'User' and inv_peers[val['entity_id']]['entity_type'] != "Channel":
            inv_peers[val['entity_id']]['entity_type'] = val['entity_type']
    return inv_peers

def update_missing_peers(df, peers_dict):
    '''Based on the users stored in peers_dict:
        - Fill in the missing IDs in the Mentions IDs column.
        - Fill in the missing Type in the Mentions Peer Type IDs column.
        - Fill in the missing IDs in the Reactions IDs column.
        :param DataFrame df: dataframe with scraped data
        :param dict peers_dict: if provided, dictionary will store peers' info in a format: 
        userrname:{'entity_type':entity_type, 'entity_id':entity_id}'''
    usernames = df['Mentions Peer Username'].map(lambda x: str(x).rstrip().split(' ') if pd.isnull(x) == False else x)
    df['Mentions IDs'] = [' '.join(str(peers_dict[name]['entity_id']) if name in peers_dict else '<NA>' 
                         for name in usernames[i]).rstrip() if pd.isnull(ids) == False else ids
                         for (i, ids) in enumerate(df['Mentions IDs'])]
    df['Mentions Peer Type'] = [' '.join(str(peers_dict[name]['entity_type']) if name in peers_dict else '<NA>' 
                                 for name in usernames[i]).rstrip() if pd.isnull(utype) == False else utype
                                 for (i, utype) in enumerate(df['Mentions Peer Type'])]
    id_to_usr = merge_duplicate_peers(peers_dict)
    usernames = df['Reactions IDs'].map(lambda x: [list(id_to_usr[int(float(idx))]['usernames']) for  idx in str(x).rstrip().split(' ')] 
                                         if pd.isnull(x) == False else x)
    df['Reactions Peer Username'] = [' '.join(l[0] if l else '<NA>' for l in usernames[i]).rstrip() if pd.isnull(name) == False else name
                                     for (i, name) in enumerate(df['Reactions Peer Username'])]