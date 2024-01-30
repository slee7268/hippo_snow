import json
import os
# Assuming you have a module named snow_db_utils.py with the following functions:
# connect_to_snow_db, get_call_data_from_call_id, get_app_logs_from_call_id, get_transcript_with_votes_from_call_id
from snow_db_utils import (
    connect_to_snow_db, 
    get_call_data_from_call_id, 
    get_app_logs_from_call_id, 
    get_transcript_with_votes_from_call_id,
    get_transcript_with_dob_engine,
    get_transcript_from_call_id,
    assemble_transcript
)

def filter_transcript(conv_turns):
    """
    Filters out conversation turns where the speaker is 'system'.
    :param conv_turns: List of conversation turns.
    :return: Filtered list of conversation turns.
    """
    return [turn for turn in conv_turns if turn.get('speaker') != 'system']

def get_snippet_around_condition(call_id, window_size, condition, connection):
    """
    Gets a snippet of conversation transcript based on a specific condition.

    :param call_id: The ID of the call.
    :param window_size: The number of utterances before the condition to include.
    :param condition: A function that takes an utterance and returns True if it meets the condition.
    :param connection: Database connection object.
    :return: List of snippets, each is a list of utterances.
    """
    # Get all utterances
    all_utterances = get_transcript_from_call_id(call_id, connection)

    # Find utterances that meet the condition
    condition_indices = [i for i, utt in enumerate(all_utterances) if condition(utt)]

    # Retrieve snippets
    snippets = []
    for index in condition_indices:
        start = max(0, index - window_size)
        snippet = all_utterances[start:index+1]
        snippets.append(snippet)

    return snippets

# Example usage
def dob_engine_condition(utterance):
    return utterance.get('annotation_engine') == 'DOBEngine'

def assemble_snippet(snippets):
    """
    Assembles a list of conversation turns from snippets of utterances.
    :param snippets: List of snippets, each a list of utterance dictionaries.
    :return: List of assembled conversation turns.
    """
    conv_turns = []
    for snippet in snippets:
        current_utterance = ''
        current_speaker = 'system'
        for utterance in snippet:
            if utterance['role'] == current_speaker:
                current_utterance += ' ' + utterance['content']
            else:
                if current_utterance:  # Avoid adding empty turns
                    utt = {
                        'speaker': current_speaker,
                        'utterance': current_utterance.strip()
                    }
                    conv_turns.append(utt)
                current_utterance = utterance['content']
                current_speaker = utterance['role']
        # Add the last utterance in the snippet
        if current_utterance:
            conv_turns.append({
                'speaker': current_speaker,
                'utterance': current_utterance.strip()
            })
    return conv_turns

def process_dob_transcripts(file_path, conn):
    window_size = 5  # Number of utterances before the condition
    output_dir = "DOB"
    os.makedirs(output_dir, exist_ok=True)

    # Load the JSON content
    with open(file_path, 'r') as file:
        call_ids = json.load(file)

    # File to record call IDs with the specified content
    for call_id in call_ids:
        if not call_id:  # Skip empty call IDs
            continue
    
        try:

            # Retrieve data with DOB engine for each call ID
            results = get_transcript_with_dob_engine(call_id, conn)
            # Process the data
            snippets = get_snippet_around_condition(call_id, window_size, dob_engine_condition, conn)
            conv_turns = assemble_snippet(snippets)
            filtered_conv_turns = filter_transcript(conv_turns)

            # Serialize to JSON
            json_data = json.dumps(filtered_conv_turns, indent=4)

            # Save to a unique file for each call ID
            file_name = f'DOB/transcript_DOB_{call_id}.json'
            with open(file_name, 'w') as file:
                file.write(json_data)

            print(f"Transcript for call ID {call_id} saved to {file_name}")
        except Exception as e:
            print(f"Error processing call ID {call_id}: {e}")
            continue




def main():
    # Establish database connection
    conn = connect_to_snow_db()

    # Specify the call ID
    call_id = "4bcf3b19-6198-476f-88e2-f5ed23a02854"

    transcript = get_transcript_from_call_id(call_id, conn)
    conv_turns = assemble_transcript(transcript)

    filtered_conv_turns = filter_transcript(conv_turns)


    json_data = json.dumps(filtered_conv_turns, indent=4)
    with open('assembled_transcripts/transcript.json', 'w') as file:
        file.write(json_data)

    results = get_transcript_with_dob_engine(call_id, conn)
    
    window_size = 5  # Number of utterances before the condition

    snippets = get_snippet_around_condition(call_id, window_size, dob_engine_condition, conn)

    conv_turns = assemble_snippet(snippets)

    filtered_conv_turns = filter_transcript(conv_turns)

    json_data = json.dumps(filtered_conv_turns, indent=4)
    with open('assembled_transcripts/transcript2.json', 'w') as file:
        file.write(json_data)    

    file_path = '1_25_mentoring_call_ids.json'

    process_dob_transcripts(file_path, conn)

    """
    # Retrieve and save call data
    call_data = get_call_data_from_call_id(call_id, conn)
    with open(f'call_data_{call_id}.json', 'w') as file:
        json.dump(call_data, file, indent=4, cls=CustomJSONEncoder)
    print(f"Call data saved to call_data_{call_id}.json")

    # Retrieve and save app logs
    app_logs = get_app_logs_from_call_id(call_id, conn)
    with open(f'app_logs_{call_id}.json', 'w') as file:
        json.dump(app_logs, file, indent=4, cls=CustomJSONEncoder)
    print(f"App logs saved to app_logs_{call_id}.json")

    # Retrieve and save transcript with votes
    transcript_with_votes = get_transcript_with_votes_from_call_id(call_id, conn)
    with open(f'transcript_with_votes_{call_id}.json', 'w') as file:
        json.dump(transcript_with_votes, file, indent=4, cls=CustomJSONEncoder)
    print(f"Transcript with votes saved to transcript_with_votes_{call_id}.json")
    """
        


if __name__ == "__main__":
    main()


