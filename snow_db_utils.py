from sqlalchemy import create_engine, text

def connect_to_snow_db():

    db_url = "eval-rds.c2xxhqohz0cm.us-west-2.rds.amazonaws.com"
    port = "5432"
    username = "postgres"
    db_name = "hippocraticdb"
    password = "6F!Eu{Mes34k"# should be env

    engine = create_engine(f"postgresql://{username}:{password}@{db_url}:{port}/{db_name}")
    conn = engine.connect()
    return conn

CONN = connect_to_snow_db()

def get_call_data_from_call_id(call_id, connection=CONN):
    query_str = f"""
    SELECT * FROM call.calls
    WHERE call_id='{call_id}'
    """

    output = connection.execute(text(query_str))
    results = output.fetchall()
    r = results[0]
    r_dict = r._asdict()
    return r_dict

def get_app_logs_from_call_id(call_id, connection):
    query_str = f"""SELECT * FROM log.app_logs
    WHERE call_id='{call_id}'
    ORDER BY log_ts ASC"""

    output = connection.execute(text(query_str))
    results = output.fetchall()
    results = [r._asdict() for r in results]

    return results

def get_transcript_with_votes_from_call_id(call_id, connection=CONN):
    query_str = f"""
    SELECT * FROM call.call_utterances
    WHERE call_id='{call_id}'
    ORDER BY timestamp ASC
    """
    # dont like ordering by timestamp, kinda soft

    output = connection.execute(text(query_str))
    results = output.fetchall()
    utterances = [r._asdict() for r in results]

    vote_query = f"""
    SELECT * FROM call.call_utterance_votes
    WHERE call_id='{call_id}'
    """

    vote_output = connection.execute(text(vote_query))
    vote_results = vote_output.fetchall()

    votes = [r._asdict() for r in vote_results]

    for utterance in utterances:
        utterance['votes'] = []
        for vote in votes:
            if utterance['utterance_id'] == vote['utterance_id']:
                #print(call_id, utterance['utterance_id'])
                utterance['votes'].append(vote)

        utterance['votes'] = [x['vote'] for x in utterance['votes']]

    return utterances

def get_votes_from_call_id(call_id, connection=CONN):

    vote_query = f"""
    SELECT * FROM call.call_utterance_votes
    WHERE call_id='{call_id}'
    """

    vote_output = connection.execute(text(vote_query))
    vote_results = vote_output.fetchall()
    votes = [r._asdict() for r in vote_results]

    return votes

def assemble_transcript_with_votes(transcript):
    # transcript is a list of utterance dicts direct from the db

    conv_turns = []
    current_utterance = ''
    current_speaker = 'system'


    for utterance in transcript:
        if utterance['role'] == current_speaker:
            current_utterance += ' ' + utterance['content']
        else:
            utt = {
                'speaker': current_speaker,
                'utterance': current_utterance.strip(),
                'votes': utterance['votes']
            }
            conv_turns.append(utt)
            current_utterance = utterance['content']
            current_speaker = utterance['role']

    return conv_turns

def get_transcript_from_call_id(call_id, connection=CONN):
    query_str = f"""
    SELECT * FROM call.call_utterances
    WHERE call_id='{call_id}'
    ORDER BY timestamp ASC
    """
    # dont like ordering by timestamp, kinda soft

    output = connection.execute(text(query_str))
    results = output.fetchall()
    results = [r._asdict() for r in results]

    return results


def assemble_transcript(transcript):
    # transcript is a list of utterance dicts direct from the db

    conv_turns = []
    current_utterance = ''
    current_speaker = 'system'


    for utterance in transcript:
        if utterance['role'] == current_speaker:
            current_utterance += ' ' + utterance['content']
        else:
            utt = {
                'speaker': current_speaker,
                'utterance': current_utterance.strip()
            }
            conv_turns.append(utt)
            current_utterance = utterance['content']
            current_speaker = utterance['role']

    return conv_turns
    
def get_transcript_with_dob_engine(call_id, connection=CONN):
    query_str = """
    SELECT * FROM call.call_utterances
    WHERE call_id=:call_id AND annotation_engine='DOBEngine'
    """
    output = connection.execute(text(query_str), {'call_id': call_id})
    results = output.fetchall()
    return [r._asdict() for r in results]


