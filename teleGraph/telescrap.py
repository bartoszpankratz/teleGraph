# telescrap.py
# Author: Bartosz Pankratz <bartosz.pankratz@gmail.com>
# Auxilliary functions used in a Telegram scraping process
#
# License: MIT License
 
# Install the Telethon library for Telegram API interactions
#!pip install -q telethon
# Telegram imports
from telethon.sync import TelegramClient
# Initial imports
import time
import logging
from teleGraph.lang_utils import remove_unsupported_characters, extract_mentions

logger = logging.getLogger(__name__)

def format_time(seconds):
    '''Function to format time (given in seconds) in days, hours, minutes, and seconds'''
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f'{int(days):02}:{int(hours):02}:{int(minutes):02}:{int(seconds):02}'
	
def print_progress(t_index, message_id, start_time, max_t_index):
    '''Function to print progress of the scraping process
        :param int t_index: number of messages already processed
        :param int message_id: id of currently processed message
        :param int start_time: start time for the scraping session
        :param int max_t_index: maximum number of messages to scrape'''
    elapsed_time = time.time() - start_time
    current_progress = t_index / (t_index + message_id) if (t_index + message_id) <= max_t_index else t_index / max_t_index
    percentage = current_progress * 100
    estimated_total_time = elapsed_time / current_progress
    remaining_time = estimated_total_time - elapsed_time
    elapsed_time_str = format_time(elapsed_time)
    remaining_time_str = format_time(remaining_time)
    print(f'Progress: {percentage:.2f}% | Elapsed Time: {elapsed_time_str} | Remaining Time: {remaining_time_str}')
	
def extract_peer_info(peer_dict):
    '''Function that extracts peer type and id from a peer_dict dictionary '''
    type = peer_dict['_'].replace('Input','')
    type = type.replace('Peer','')
    if type == 'Channel':
        id =  peer_dict['channel_id']
    elif type == 'User':
        id =  peer_dict['user_id']
    elif type == 'Chat':
        id =  peer_dict['chat_id']
    return type,id

def get_usernames(sender, peers_dict = None):
    '''Extract all usernames of Peer's (channel, user, group).
        :param sender: telethon message.sender entity
        :param dict peers_dict: if provided, dictionary will store peers' info in a format: 
        userrname:{'entity_type':entity_type, 'entity_id':entity_id} (default None)'''
    try:
        sender = sender.to_dict()
        usernames = sender['usernames'] + [sender['username'], ]
        usernames = set(map(lambda x: '@' + x, filter(lambda x: isinstance(x, str), usernames)))
        if peers_dict != None:
            for usrname in usernames:
                if usrname not in peers_dict:
                    peers_dict[usrname] = {'entity_type':sender['_'], 
                                           'entity_id':sender['id']}
        return ' '.join(name for name in usernames)
    except Exception as e:
        return '' 

def retrieve_username(entity_id, peers_dict):
    '''Function that retrieves @username given Peer ID from cache dictionary. 
        :param int entity_id: ID of a given entity  
        :param dict peers_dict: if provided, dictionary will store peers' info in a format: 
        userrname:{'entity_type':entity_type, 'entity_id':entity_id} (default None)'''
    if not isinstance(peers_dict, dict):
        return None
    keys = list(filter(lambda key: peers_dict[key]['entity_id'] == entity_id 
                       if 'entity_id' in peers_dict[key] else False, peers_dict))
    return keys[0] if keys else None
    
def extract_reactions(message, peers_dict = None):
    '''Function that extracts reactions from a message
        :param Message message: Message Telethon entity
        :param dict peers_dict: if provided, dictionary will store peers' info in a format: 
        userrname:{'entity_type':entity_type, 'entity_id':entity_id} (default None)'''
    emoji_string = ''
    reaction_ids = ''
    reaction_peer_username = ''
    reaction_peer_type = ''
    try:
        if message.reactions:
            #check if recent reactions are avalaible:
            if message.reactions.recent_reactions:
                for reaction_count in message.reactions.recent_reactions:
                    emoji = reaction_count.reaction.emoticon
                    emoji_string += emoji + " " + "1" + " "
                    type, id = extract_peer_info(reaction_count.peer_id.to_dict())
                    username = retrieve_username(id, peers_dict) 
                    if not username:
                        username = '<NA>'
                    reaction_peer_username += username + " " 
                    reaction_ids += str(id) + " " 
                    reaction_peer_type += type + " " 
            else:
                for reaction_count in message.reactions.results:
                    emoji = reaction_count.reaction.emoticon
                    count = str(reaction_count.count)
                    emoji_string += emoji + " " + count + " "
    except Exception as e:
        pass
        #logger.error('Error processing reactions:')
        #logger.error(e)
        #print(f'Error processing reactions: {e}')
    return emoji_string, reaction_ids, reaction_peer_username, reaction_peer_type	

#This version of extract_mentioned_peers function is the most effective and complete
#(it extents the graph with all channels and users mentioned inside the posts).
#However, Telegram limits username resolve to ~200 entities. As a result,
#this function might easily cause a FloodWaitError and temporary ban (up to 24h!)
#by sending too many requests via get_input_entity method.
#We recommend using the simplified version which does not involve additonal Telegram API requests.

#async def extract_mentioned_peers(client, text, peers_dict = None):
#    '''Function that extracts the mentions (in the form of @username or https://t.me/username)
#       from the post content and retrieves the ID and peer type of the author. 
#        :param client: telethon client
#        :param str text: content of a scraped message 
#       :param dict peers_dict: if provided, dictionary will store peers' info in a format: 
#        userrname:{'entity_type':entity_type, 'entity_id':entity_id} (default None)'''
#    #create local cache dictionary for users if global is not provided:
#    if peers_dict == None: 
#        peers_dict = dict()
#    #add key for unused peers storage:
#    if 'unused peers' not in peers_dict:
#        peers_dict['unused peers'] = set()
#    #declare variables:
#    mentioned_peers, mentioned_peer_type, mentioned_ids = '', '', ''
#    #extract mentions:
#    mentions = extract_mentions(text)
#    for mention in mentions:
#        try:
#            #if mention in unused peers - omit:
#            if mention in peers_dict['unused peers']:
#                continue
#            #if mention already in peers_dict - add data to results:
#            elif mention in peers_dict:
#                mentioned_peers += mention + ' '
#                mentioned_peer_type += peers_dict[mention]['entity_type'] + ' '
#                mentioned_ids += str(peers_dict[mention]['entity_id']) + ' '
#            #if mention not in peers dict - scrape and add to dictionary:
#            elif mention not in peers_dict:
#                entity = await client.get_input_entity(mention)
#                entity_type, entity_id = extract_peer_info(entity.to_dict())
#                mentioned_peers += mention + ' '
#                mentioned_peer_type += entity_type + ' '
#                mentioned_ids += str(entity_id) + ' '
#                peers_dict[mention] = {'entity_type':entity_type, 
#                                       'entity_id':entity_id}
#        except Exception as e:
#            #add faulty peer to unused peers key in the dictionary:
#            if mention not in peers_dict['unused peers']:
#                peers_dict['unused peers'].add(mention)
#            logger.error(f'Error processing mentions of {mention}:')
#            logger.error(e)
#            #print(f'Error processing mentions of {mention}: {e}') 
#    return mentioned_peers, mentioned_peer_type, mentioned_ids

#Simplified version of extract_mentioned_peers function.
#This version does not send Telegram API requests via get_input_entity method,
#thus it does not cause a FloodWaitError. However, scraped data is less accurate
#and might cause problems with graph generation. 
#We recommend two approaches for utilizing the mentions in a graph:
#1. Use peer's name instead of ID: in this scenario graph will include all mentions as nodes, 
#but might be inaccurate - some nodes can be duplicated, once created with peer's ID (from mention)
#and once with message/coment/reaction (created with ID).
#2. Use only certain nodes stored in an external peers dictionary. In this case, mentions are only 
#mapped for nodes that are stored in a premade dictionary that stores nodes in a format:
#userrname:{'entity_type':entity_type, 'entity_id':entity_id}
#In this scenario, nodes will be matched exactly, 
#but large number of them will not be included in a graph.
#In the experiments provided with this library, we use the latter solution.  

def extract_mentioned_peers(text, peers_dict = None):
    '''Function that extracts the mentions (in the form of @username or https://t.me/username)
       from the post content and retrieves the ID and peer type of the author (if possible). 
        :param str text: content of a scraped message 
        :param dict peers_dict: if provided, dictionary will store peers' info in a format: 
        userrname:{'entity_type':entity_type, 'entity_id':entity_id} (default None)'''
    #declare variables:
    mentioned_peers, mentioned_peer_type, mentioned_ids = '', '', ''
    #extract mentions:
    mentions = extract_mentions(text)
    for mention in mentions:
        try:
            peer_type, peer_id = '<NA> ', '<NA> '
            if peers_dict != None:
                #if mention already in peers_dict - add data to results:
                if mention in peers_dict:
                    peer_type = peers_dict[mention]['entity_type'] + ' '
                    peer_id = str(peers_dict[mention]['entity_id']) + ' '
            #if mention not in peers dict - add peer without entity_type and entity_type (only @username):
            mentioned_peers += mention + ' '
            mentioned_peer_type += peer_type
            mentioned_ids += peer_id
        except Exception as e:
            logger.error(f'Error processing mentions of {mention}:')
            logger.error(e)
            #print(f'Error processing mentions of {mention}: {e}') 
    return mentioned_peers, mentioned_peer_type, mentioned_ids
     
async def extract_data_from_message(client, message, channel, respond_to_id, peers_dict = None):
    '''Function that extracts crucial elements of the message
        :param client: telethon client
        :param Message message: Message Telethon entity
        :param str channel: name of the channel
        :param int respond_to_id: id of the responded message
        :param dict peers_dict: if provided, dictionary will store peers' info in a format: 
        userrname:{'entity_type':entity_type, 'entity_id':entity_id} (default None)'''
    #change date format
    date_time = message.date.strftime('%Y-%m-%d %H:%M:%S')
    #extract info about post author
    if message.from_id != None:
        author_type, author_id = extract_peer_info(message.from_id.to_dict())
    else:
        author_type, author_id = extract_peer_info(message.peer_id.to_dict())
    #extract all author username(s):
    username = get_usernames(message.sender, peers_dict = peers_dict)
    #clean text
    if message.text != None:
        cleaned_content = remove_unsupported_characters(message.text)
        #extract mentions:
        #version 1 (with API requests):
        #(mentioned_peer_names, mentioned_peer_type, 
        # mentioned_ids) = await extract_mentioned_peers(client, cleaned_content, peers_dict = peers_dict)
        #version 2 (simplified):
        (mentioned_peer_names, mentioned_peer_type, 
         mentioned_ids) = extract_mentioned_peers(cleaned_content, peers_dict = peers_dict)
    else:
        cleaned_content= ""
        mentioned_peer_names, mentioned_peer_type, mentioned_ids = '', '', ''
    #check if media
    media = 'True' if message.media else 'False'
    #working on reactions to the post
    (emoji_string, reaction_ids, 
     reaction_peer_username, reaction_peer_type) = extract_reactions(message, peers_dict = peers_dict)
    #extract info about forwarded messages
    try:
        fwd_id = message.fwd_from.channel_post if message.fwd_from.channel_post != None else ''
        fwd_type, fwd_author_id = extract_peer_info(message.fwd_from.from_id.to_dict())
        fwd_username = retrieve_username(fwd_author_id, peers_dict)
        if not fwd_username:
            fwd_message = await client.get_messages(fwd_author_id, ids = fwd_id)
            fwd_username = get_usernames(fwd_message.sender, peers_dict = peers_dict)
        fwd_date  = (message.fwd_from.date.strftime('%Y-%m-%d %H:%M:%S') 
                     if message.fwd_from.date != None else '')    
    except Exception as e:
        #logger.error('Error processing message:')
        #logger.error(e)
        #print(f'Error processing message: {e}')
        fwd_id, fwd_author_id, fwd_username, fwd_type, fwd_date = None, None, "", "", ""            
    #extract info about the message responding to    
    if message.reply_to != None: 
        reply_id = int(message.reply_to.reply_to_top_id or 2147483646)
        reply_id = min(message.reply_to.reply_to_msg_id, reply_id)
        reply_id = int(respond_to_id or reply_id)
        prev_message = await client.get_messages(channel, ids = reply_id)
        if prev_message != None:
            prev_message = prev_message[0] if isinstance(prev_message, list) else prev_message
            prev_message_username = get_usernames(prev_message.sender, peers_dict = peers_dict)
            prev_message_date  = prev_message.date.strftime('%Y-%m-%d %H:%M:%S') if prev_message.date != None else ''
            if prev_message.from_id != None:
                prev_message_type, prev_message_author_id = extract_peer_info(prev_message.from_id.to_dict())
            elif prev_message.peer_id != None:
                prev_message_type, prev_message_author_id = extract_peer_info(prev_message.peer_id.to_dict()) 
            else:
                prev_message_type, prev_message_author_id = "", None
        else:
            prev_message_username, prev_message_type = "", ""
            prev_message_author_id, prev_message_date = None, ""
    else:
        reply_id, prev_message_username, prev_message_type = None, "", ""
        prev_message_author_id, prev_message_date = None, ""
    return {
        'Message ID': message.id,
        'Author ID': author_id,
        'Author Username': username,
        'Author Alias': message.post_author if message.post_author != None else '',
        'Author Type': author_type,
        'Date': date_time,
        'Channel': channel,
        'Type': "Message",
        'Views': message.views,
        'Shares': message.forwards,
        'Reply to ID': reply_id,
        'Reply to Author ID': prev_message_author_id,
        'Reply to Username': prev_message_username, 
        'Reply to Author Type': prev_message_type,
        'Reply to Date': prev_message_date,
        'Forwarded from Post ID': fwd_id,
        'Forwarded from Author ID': fwd_author_id,
        'Forwarded from Author Username': fwd_username,
        'Forwarded from Author Type': fwd_type,
        'Forwarded from Post Date': fwd_date,
        'Reactions': emoji_string,
        'Reactions IDs': reaction_ids,
        'Reactions Peer Username': reaction_peer_username, 
        'Reactions Peer Type': reaction_peer_type,
        'Mentions IDs': mentioned_ids,
        'Mentions Peer Username': mentioned_peer_names,
        'Mentions Peer Type': mentioned_peer_type,
        'Content': cleaned_content,
        'Media': media,
        'Url': f'https://t.me/{channel}/{message.id}'.replace('@', '')
    }

#Warning! extract_peer_data function might cause a FloodWaitError!
#Use cautiously!
	
async def extract_peer_data(client, id, entities):
    '''Function that extracts Peer's (channel, user, group) full data
        :param client: telethon client
        :param int id: id of entity
        :param dict entities: dictionary containing all entities'''
    try: 
        entity = await client.get_entity(id)
        entity = entity.to_dict()
        entity_type = entity['_']
        del entity['_']
        if not entity_type in entities.keys():
            entities[entity_type] = dict()
        entity_id = entity['id']
        if not entity_id in entities[entity_type]:
            del entity['id']
            entities[entity_type][entity_id] = entity
    except Exception as e:
        logger.error(f'entity {id} error:')
        logger.error(e) 
        print(f'entity {id} error: {e}')
	
