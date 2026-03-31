# subgraphs.py
# Author: Bartosz Pankratz <bartosz.pankratz@gmail.com>
# Graph extraction; 
# From full graph edgelist exctract subgraph edges centered around single post or user.
# Warning! Works only for edgelist with interaction type columns, namely variant 'type' or 'all'
# of the edgelist.
#
# License: MIT License

import pandas as pd

def get_all_peers_posts(peer_id, post_id, visited_posts, edgelist_full, target_only = True, get_all = True):
    '''Extract subnetwork centred around a given post. When get_all = True,
       function will recursively obtain subgraphs for all neighbors of the given post 
       and add them to the edgelist. 
        :param str peer_id: id of a peer (user or channel).
        :param str post_id: id of a post. 
        :param set visited_posts: set of tuples with already visited posts.
        :param pandas.DataFrame edgelist_full: DataFrame with full graph edgelist.
         Warning! Works only for edgelist with interaction type columns, 
         namely variant 'type' or 'all' of the edgelist.
        :param bool target_only (default True): when True, get only direct interactions with a post,
        namely, replies to it and its forwards (edges where post is a target node). 
        When False, get also posts that a given post is replying to and a source message, if the post was a forward itself. 
        :param bool get_all (default True): when True, get all neighbors of post's neighbors recursively.
    '''
    #get all reactions to this post:
    df = edgelist_full[(edgelist_full['target'] == peer_id) & 
        (edgelist_full['target post ID'] == post_id) & (edgelist_full['reaction'] == 1)].reset_index(drop=True)
    #get all channels mentioned in this post:
    auxdf = edgelist_full[(edgelist_full['source'] == peer_id) &
            (edgelist_full['source post ID'] == post_id) & (edgelist_full['mention'] == 1)].reset_index(drop=True)
    df = pd.concat([df,auxdf], axis = 0)
    #forwards and replies - look at both, if this post was forwarded (or was a forward) and if post is a reply (or someone replied to it)
    peers_idxs = ((edgelist_full['forward'] == 1) | (edgelist_full['reply'] == 1))
    filtered_idxs = ((edgelist_full['target'] == peer_id) & (edgelist_full['target post ID'] == post_id))
    if not target_only:
        filtered_idxs = filtered_idxs | ((edgelist_full['source'] == peer_id) & (edgelist_full['source post ID'] == post_id))
    peers_idxs = peers_idxs & filtered_idxs
    auxdf = edgelist_full[peers_idxs].reset_index(drop=True)
    df = pd.concat([df,auxdf], axis = 0)
    #dug deeper for peers:
    if get_all:
        targets = df[(df['target'] != peer_id) & 
                    (df['target post ID'] != post_id)][['target', 'target post ID']].itertuples(index=False, name=None)
        iterators = [targets,]
        if not target_only:
            sources = df[(df['source'] != peer_id) & 
                    (df['source post ID'] != post_id)][['source', 'source post ID']].itertuples(index=False, name=None)
            iterators.append(sources)
        for iterator in iterators:
            for (peer_idx, post_idx) in iterator:
                if (peer_idx, post_idx) in visited_posts:
                    continue
                visited_posts.add((peer_idx, post_idx))
                auxdf = get_all_peers_posts(peer_idx, post_idx, visited_posts, edgelist_full,
                                           target_only = target_only, get_all = get_all)
                df = pd.concat([df,auxdf], axis = 0)
    return df.drop_duplicates()

def get_posts_subgraph(post_pairs, edgelist_full, target_only = True, get_all = True):
    '''For a given collection of posts, get a subgraph induced by them. 
       When get_all = True, function will recursively obtain subgraphs 
       for all neighbors of the given post and add them to the edgelist. 
        :param iterable post_pairs: iterable  containing tuples (peer_id, post_id), necessary 
        for network extraction.
        :param pandas.DataFrame edgelist_full: DataFrame with full graph edgelist.
         Warning! Works only for edgelist with interaction type columns, 
         namely variant 'type' or 'all' of the edgelist.
        :param bool target_only (default True): when True, get only direct interactions with a post,
        namely, replies to it and its forwards (edges where post is a target node). 
        When False, get also posts that a given post is replying to and a source message, if the post was a forward itself. 
        :param bool get_all (default True): when True, get all neighbors of post's neighbors recursively.
    '''
    visited_posts = set(post_pairs)
    fin_df = pd.DataFrame()
    for (peer_id, post_id) in visited_posts.copy():
        auxdf = get_all_peers_posts(peer_id, post_id, visited_posts, 
                                    edgelist_full, target_only = target_only, get_all = get_all)
        fin_df = pd.concat([fin_df, auxdf], axis = 0).drop_duplicates()
    return fin_df

def get_peer_subgraph(peer_id, edgelist_full, target_only = True, get_all = True):
    '''For a given peer ID, get a subgraph induced by its posts. 
       When get_all = True, function will recursively obtain subgraphs 
       for all neighbors of the given post and add them to the edgelist. 
        :param int peer_id: ID of a peer.
        :param pandas.DataFrame edgelist_full: DataFrame with full graph edgelist.
         Warning! Works only for edgelist with interaction type columns, 
         namely variant 'type' or 'all' of the edgelist.
        :param bool target_only (default True): when True, get only direct interactions with a post,
        namely, replies to it and its forwards (edges where post is a target node). 
        When False, get also posts that a given post is replying to and a source message, if the post was a forward itself. 
        :param bool get_all (default True): when True, get all neighbors of post's neighbors recursively.
    '''
    visited_posts = set()
    df = edgelist_full[(edgelist_full['source'] == peer_id) | (edgelist_full['target'] == peer_id)].reset_index(drop=True)
    fin_df = df.copy()
    visited_posts = set(df[['source', 'source post ID']].itertuples(index=False, name=None))
    visited_posts.update(set(df[['target', 'target post ID']].itertuples(index=False, name=None)))
    #reactions are done, no way we can scrape them further
    #mentions - filter out mentions made by given channel, look at posts that mention this channel
    mentions = df[(df['mention'] == 1) & 
                    (df['target'] == peer_id)][['source', 'source post ID']].itertuples(index=False, name=None)
    for (peer_idx, post_idx) in mentions:
        visited_posts.add((peer_idx, post_idx))
        #get all interactions with posts mentioning channel:
        auxdf = get_all_peers_posts(peer_idx, post_idx, visited_posts, 
                                    edgelist_full, target_only = target_only, get_all = get_all)
        fin_df = pd.concat([fin_df, auxdf], axis = 0).drop_duplicates()
    #forwards and replies - look at both, if this post was forwarded
    #(or was a forward) and if the post is a reply (or someone replied to it)
    peers_idxs = ((edgelist_full['forward'] == 1) | (edgelist_full['reply'] == 1))
    filtered_idxs = (edgelist_full['target'] == peer_id)
    #filter all posts where peer is a target node:
    sources = edgelist_full[peers_idxs & 
                            filtered_idxs][['source', 'source post ID']].itertuples(index=False, name=None)
    iterators = [sources, ]
    #look also at posts that peer is pointing to:
    if not target_only:
        filtered_idxs = (edgelist_full['source'] == peer_id) 
        targets = edgelist_full[peers_idxs &
                                filtered_idxs][['target', 'target post ID']].itertuples(index=False, name=None)
        iterators.append(targets)
    for iterator in iterators:
        for (peer_idx, post_idx) in iterator:
            auxdf = get_all_peers_posts(peer_idx, post_idx, visited_posts, 
                                        edgelist_full, target_only = target_only, get_all = get_all)
            fin_df = pd.concat([fin_df, auxdf], axis = 0).drop_duplicates()
    return fin_df

