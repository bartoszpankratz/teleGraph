# edgelist.py
# Author: Bartosz Pankratz <bartosz.pankratz@gmail.com>
# Exctract edglist and metadata from scraped files.
#
# License: MIT License

import os
import logging
import warnings
import re
import pandas as pd
from collections import defaultdict, Counter

logger = logging.getLogger(__name__)

def create_peer_metadata_instance():
    '''Create empty peer (channel or user)'''
    return {
            'type': "Unknown",             #type of the peer: channel or user
            'username': "None",             #name of the channel
            'active in': set(),     #channels and groups where user is active
            'languages': Counter(),        #languages used by channel or peer
        
            'no of messages': 0,    #number of published messages
            'no of views': 0,       #how many times posts were viewed
            'no of shares': 0,      #how many times posts were shared
            'engagement': 0,   #number of reactions to the posts of the user
            'no of reactions': 0,   #number of reactions to the posts given by user
            'no of mentions': 0,   #number of mentions of the user
        
            'no of comments': 0,    #number of replies user's posts got
            'no of forwarded': 0,   #how many times user's posts were forwarded 
            'no of replies': 0,     #number of replies user gave
            'no of forwards': 0,    #how many times peer forward posts
           }
           
def create_peer_metadata():
    '''Create empty peers (channel or user) metadata dictionary'''
    return defaultdict(lambda: create_peer_metadata_instance())
    
def get_peers_metadata(df, peers_metadata):
    ''' Update peers metadata dictionary with peers from a given DataFrame'''
    for (i, row) in df.iterrows():
        peer = int(float(row['Author ID']))
        peers_metadata[peer]['type'] = row['Author Type']
        peers_metadata[peer]['username'] = row['Author Username'] if not pd.isnull(row['Author Username']) else 'None'
        peers_metadata[peer]['active in'].add(row['Channel'])
        if row['Language']:
            peers_metadata[peer]['languages'][row['Language']] += 1  
        peers_metadata[peer]['no of messages'] += 1
        peers_metadata[peer]['no of views'] += int(row['Views'] if not  pd.isnull(row['Views'])  else 0) 
        peers_metadata[peer]['no of shares'] += int(row['Shares'] if not pd.isnull(row['Shares']) else 0)
        peers_metadata[peer]['engagement'] += sum(int(i) for i in  str(row['Reactions']).split() if i.isdecimal())
        #replies
        if not pd.isnull(row['Reply to Author ID']):
            aux_peer = int(float(row['Reply to Author ID']))
            peers_metadata[peer]['no of replies'] += 1
            peers_metadata[aux_peer]['no of comments'] += 1
            peers_metadata[aux_peer]['type'] = row['Reply to Author Type']
            peers_metadata[aux_peer]['username'] = row['Reply to Username'] if not pd.isnull(row['Reply to Username']) else 'None'
        #forwards
        if not pd.isnull(row['Forwarded from Author ID']):
            aux_peer = int(float(row['Forwarded from Author ID']))
            peers_metadata[peer]['no of forwards'] += 1
            peers_metadata[aux_peer]['no of forwarded'] += 1
            peers_metadata[aux_peer]['type'] = row['Forwarded from Author Type']
            peers_metadata[aux_peer]['username'] = row['Forwarded from Author Username'] if not pd.isnull(row['Forwarded from Author Username']) else 'None'
        #mentions:
        if not pd.isnull(row['Mentions IDs']):
            mentions_ids = str(row['Mentions IDs']).rstrip().split(' ')
            mentions_names = str(row['Mentions Peer Username']).rstrip().split(' ')
            mentions_types = str(row['Mentions Peer Type']).rstrip().split(' ')
            for (i, aux_peer) in enumerate(mentions_ids):
                if aux_peer == '<NA>':
                    continue
                aux_peer = int(float(aux_peer))
                peers_metadata[aux_peer]['no of reactions'] += 1
                peers_metadata[aux_peer]['type'] = mentions_types[i] if not mentions_types[i] != '<NA>' else 'Unknown'
                peers_metadata[aux_peer]['username'] = mentions_names[i] if not mentions_names[i] != '<NA>' else 'None'
        #reactions:
        if not pd.isnull(row['Reactions IDs']):
            reactions_ids = str(row['Reactions IDs']).rstrip().split(' ')
            reactions_names = str(row['Reactions Peer Username']).rstrip().split(' ')
            reactions_types = str(row['Reactions Peer Type']).rstrip().split(' ')
            for (i, aux_peer) in enumerate(reactions_ids):
                if aux_peer == '<NA>':
                    continue
                aux_peer = int(float(aux_peer))
                peers_metadata[aux_peer]['no of mentions'] += 1
                peers_metadata[aux_peer]['type'] = reactions_types[i] if not reactions_types[i] != '<NA>' else 'Unknown'
                peers_metadata[aux_peer]['username'] = reactions_names[i] if not reactions_names[i] != '<NA>' else 'None'
                

def filter_edges_from_df(df, source_col, target_col, auxiliary_cols, interacion_type, include_type = True):
    ''' Filter all the edges of a given type from DataFrame object.  
        :param pandas.DataFrame df: DataFrame with Telegram Post 
        :param str source_col: Edge source column name
        :param str target_col: Edge target column name
        :param dict auxiliary_cols: Dictionary with auxiliary columns you want to include in edglist.
         Stored in a format: {new_column_name: df_column_name} 
        :param str interacion_type {'reply', 'forward', 'reaction', 'mention'}:
            Determines the type of interaction between peers you want to filter out.
            - 'reply': used for extracting post replies
            - 'forward': used for extracting post forwards
            - 'reaction': used for extracting peers from reactions 
            - 'mention': used for extracting peers from mentions
        :param bool include_type (default True): include type of interaction in final dataset
    '''
    valid_interactions = ['reply', 'forward', 'reaction', 'mention']
    if interacion_type not in valid_interactions:
        raise ValueError(f"'{interacion_type}' not understood, please provide one of {valid_interactions}")
    #filter interactions:
    filtered_df = df[(df[source_col].isnull() == False) & (df[target_col].isnull() == False)].reset_index(drop=True)
    edge_df = filtered_df[[source_col, target_col]].rename(columns = {source_col: 'source', target_col: 'target'})
    #add valid columns:
    for (new_col, col) in auxiliary_cols.items():
        edge_df[new_col] = filtered_df[col] 
    #explode and handle reactions and mentions:
    if interacion_type == 'reaction':
        edge_df['source'] = edge_df['source'].map(lambda x: str(x).rstrip().split(' '))
        edge_df = edge_df.explode('source')
        edge_df = edge_df[edge_df['source'] != '<NA>'].reset_index(drop=True)
        edge_df['source'] = edge_df['source'].astype('float64').astype('Int64')
    elif interacion_type == 'mention':
        edge_df['target'] = edge_df['target'].map(lambda x: str(x).rstrip().split(' '))
        edge_df = edge_df.explode('target')
        edge_df = edge_df[edge_df['target'] != '<NA>'].reset_index(drop=True)
        edge_df['target'] = edge_df['target'].astype('float64').astype('Int64')
        #filter out self-mentions:
        edge_df = edge_df[edge_df['target'] != edge_df['source']].reset_index(drop=True)
    if include_type:
        #encode interaction dummy variable:
        interaction_type = pd.Categorical([interacion_type,] * edge_df.shape[0], categories=valid_interactions)
        #concatenate dataframe:
        edge_df = pd.concat([edge_df, pd.get_dummies(interaction_type, dtype = int)], axis = 1)
        #change column order:
        edge_df = edge_df[["source", "target", *valid_interactions, *auxiliary_cols.keys()]]
    return edge_df

def get_edges_from_df(df, source_col, target_col, auxiliary_cols, interacion_type, variant = 'all'):
    ''' Filter all the edges of a given type from DataFrame object.  
        :param pandas.DataFrame df: DataFrame with Telegram Post 
        :param str filename (default None): Valid file name. If provided, the function will save edgelist to the file.
         If not it will return DataFrame. 
        :param str interacion_type {'reply', 'forward', 'reaction', 'mention'}:
            Determines the type of interaction between peers you want to filter out.
            - 'reply': used for extracting post replies
            - 'forward': used for extracting post forwards
            - 'reaction': used for extracting peers from reactions 
            - 'mention': used for extracting peers from mentions
        :param str variant {'simple', 'type', 'time', 'all'}:
            Determines the structure of the edglist.
            - 'simple': return only edgelist, without metadata
            - 'type': return edgelist + interaction type (reply, forward, reaction, mention) for each edge 
            - 'time': return edgelist + date of the post
            - 'all': return edgelist + interaction type + date of the post 
    '''
    if variant not in {'simple', 'type', 'time', 'all'}:
        raise ValueError(f"'{variant}' not understood, please provide one of [{'simple', 'type', 'time', 'all'}]")
    valid_interactions = ['reply', 'forward', 'reaction', 'mention']
    if interacion_type not in valid_interactions:
        raise ValueError(f"'{interacion_type}' not understood, please provide one of {valid_interactions}")
    if variant == 'simple':
        return filter_edges_from_df(df, source_col, target_col, {}, interacion_type, include_type = False)
    else:
        if variant != 'type':
            auxiliary_cols["date"] =  'Date'
        include_type = False if variant == 'time' else True
        return filter_edges_from_df(df, source_col, target_col, auxiliary_cols, interacion_type, include_type = include_type)
        
def edgelist_from_df(df, variant = 'all'):
    ''' Get all edges from a given DataFrame.  
        :param pandas.DataFrame df: DataFrame with Telegram Post 
        :param str variant {'simple', 'type', 'time', 'all'}:
            Determines the structure of the edglist.
            - 'simple': return only edgelist, without metadata
            - 'type': return edgelist + interaction type (reply, forward, reaction, mention) for each edge 
            - 'time': return edgelist + date of the post
            - 'all': return edgelist + interaction type + date of the post 
    '''
    if variant not in {'simple', 'type', 'time', 'all'}:
        raise ValueError(f"'{variant}' not understood, please provide one of [{'simple', 'type', 'time', 'all'}]") 
    edge_df = pd.DataFrame()
    #get replies:
    source_col, target_col = 'Author ID', 'Reply to Author ID'
    auxiliary_cols = {"source post ID": 'Message ID', "target post ID": 'Reply to ID'}
    edge_df = pd.concat([edge_df, get_edges_from_df(df, source_col, target_col, auxiliary_cols, "reply", variant = variant)], axis = 0)    
    #get forwards:
    source_col, target_col = 'Author ID', 'Forwarded from Author ID'
    auxiliary_cols = {"source post ID": 'Message ID', "target post ID": 'Forwarded from Post ID'}
    edge_df = pd.concat([edge_df, get_edges_from_df(df, source_col, target_col, auxiliary_cols, "forward", variant = variant)], axis = 0) 
    #get reactions:
    source_col, target_col = 'Reactions IDs', 'Author ID'
    auxiliary_cols = {"source post ID": 'Message ID', "target post ID": 'Message ID'}
    edge_df = pd.concat([edge_df, get_edges_from_df(df, source_col, target_col, auxiliary_cols, "reaction", variant = variant)], axis = 0) 
    #get mentions:
    source_col, target_col = 'Author ID', 'Mentions IDs'
    auxiliary_cols = {"source post ID": 'Message ID', "target post ID": 'Message ID'}
    edge_df = pd.concat([edge_df, get_edges_from_df(df, source_col, target_col, auxiliary_cols, "mention", variant = variant)], axis = 0) 
    return edge_df

def get_edgelist(source, nodes_metadata = None, respath = None, filenames = None, variant = 'all'):
    ''' Get all edges from a given DataFrame.  
        :param pandas.DataFrame or str source: source of the edglist.
        :param defaultdict nodes_metadata (default None): dictionary with nodes metadata.
        :param str respath (default None): filename for saving data. If not provided, the function returns a DataFrame.
        Provide a DataFrame with a Telegram Post, a file containing a DataFrame, or a directory containing multiple DataFrames. 
        :param str filenames: compiled regex for valid filenames in directory - necessary for filtering files in data directory.
        :param str variant {'simple', 'type', 'time', 'all'}:
            Determines the structure of the edglist.
            - 'simple': return only edgelist, without metadata
            - 'type': return edgelist + interaction type (reply, forward, reaction, mention) for each edge 
            - 'time': return edgelist + date of the post
            - 'all': return edgelist + interaction type + date of the post 
    '''
    if variant not in {'simple', 'type', 'time', 'all'}:
        raise ValueError(f"'{variant}' not understood, please provide one of [{'simple', 'type', 'time', 'all'}]") 
    if respath:
        # check if file already exists, if not create  and add headers 
        if not os.path.isfile(respath):
            colnames = ['source', 'target']
            if variant == 'type' or variant == 'all':
                colnames += ['reply', 'forward', 'reaction', 'mention']
            if variant != 'simple':
                colnames += ["source post ID", "target post ID"]
            if variant == 'time' or variant == 'all':
                colnames += ['date',]
            #create a file
            placeholder_df = pd.DataFrame(columns = colnames)
            placeholder_df.to_csv(respath, index = False, mode='w', sep = ';')
    if isinstance(source, pd.DataFrame):
        if nodes_metadata != None:
            get_peers_metadata(source, nodes_metadata)
        edgelist = edgelist_from_df(source, variant)
        edgelist = edgelist.astype({ x:'Int64' for x in edgelist.select_dtypes(include = 'number').columns})
        if respath:
            edgelist.to_csv(respath, index = False, header = False, mode='a', sep = ';')
            return None
        else:
            return edgelist
    elif os.path.isfile(source):
        file_extension = source.rsplit('.', 1)[-1]
        #open file:
        if file_extension == 'xlsx':
           df = pd.read_excel(source, engine='openpyxl')
        elif file_extension == 'parquet':
            df = pd.read_parquet(source, engine='pyarrow')
        elif file_extension == 'csv':
             df = pd.read_csv(source, sep = ';', encoding = 'utf=8')
        else: 
            raise ValueError("Wrong file format! Files can be stored as xlsx, parquet or csv files.")
        if df.empty:
            print(f'{source} file is empty!')
            logger.warning(f'{source} file is empty!')
            return None  
        return get_edgelist(df, nodes_metadata = nodes_metadata, respath = respath, variant = variant)
    elif os.path.isdir(source):
        if not filenames:
            raise ValueError(f"RegEx with valid filenames not provided!") 
        edgelist_df = pd.DataFrame()
        for fname in os.listdir(source):
            #extract name and id from valid files:
            matched = filenames.search(fname)
            if not matched:
                continue
            edge_df = get_edgelist(source + fname, nodes_metadata = nodes_metadata, respath = respath, variant = variant)
            if isinstance(edge_df, pd.DataFrame):
                edgelist_df = pd.concat([edgelist_df, edge_df], axis = 0)  
        return edgelist_df if not respath else None
    else:
        raise ValueError(f"Provided input is not a Dataframe, valid file or directory!") 