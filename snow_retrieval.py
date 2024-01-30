import json
from uuid import UUID
from datetime import datetime
import os
import json


# Define a custom JSON encoder to handle non-serializable objects
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

# Assuming you have a module named snow_db_utils.py with the following functions:
# connect_to_snow_db, get_call_data_from_call_id, get_app_logs_from_call_id, get_transcript_with_votes_from_call_id
from snow_db_utils import (
    connect_to_snow_db, 
    get_call_data_from_call_id, 
    get_app_logs_from_call_id, 
    get_transcript_with_votes_from_call_id,
    get_transcript_with_dob_engine
)

def save_filtered_transcripts(call_id, conn):
    # Create the DOB directory if it doesn't exist
    output_dir = "DOB"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

        # Retrieve filtered transcripts
    transcripts = get_transcript_with_dob_engine(call_id, conn)
    # Filter transcripts based on the content
    filtered_transcripts = [
        transcript for transcript in transcripts
        if transcript['content'] == "<TASK>Thank the user for confirming their date of birth and continue with the conversation.</TASK>"
    ]

    # Define the output file path
    output_file_path = os.path.join(output_dir, f"{call_id}_DOBEngine_filtered.json")
        
        # Save the transcripts to a JSON file
    with open(output_file_path, 'w') as outfile:
        json.dump(filtered_transcripts, outfile, indent=4, cls=CustomJSONEncoder)
        
    print(f"Filtered transcripts saved to {output_file_path}")


# Assuming the get_transcript_with_dob_engine function is already defined and imports are handled

def process_call_ids(call_ids, conn):
    # Directory for filtered transcripts
    output_dir = "DOB"
    os.makedirs(output_dir, exist_ok=True)
    
    # File to record call IDs with the specified content
    matching_call_ids_file = os.path.join(output_dir, "matching_call_ids.txt")
    
    with open(matching_call_ids_file, 'w') as call_ids_file:
        for call_id in call_ids:
            if not call_id:  # Skip empty call IDs
                continue

            try:
                transcripts = get_transcript_with_dob_engine(call_id, conn)  # Ensure conn is defined and available
                # Filter for the specific task content
                filtered_transcripts = [
                    t for t in transcripts if t['content'] == "<TASK>Thank the user for confirming their date of birth and continue with the conversation.</TASK>"
                ]

                if filtered_transcripts:
                    # Save the filtered transcript to a file
                    output_file_path = os.path.join(output_dir, f"{call_id}_DOBEngine_filtered.json")
                    with open(output_file_path, 'w') as outfile:
                        json.dump(filtered_transcripts, outfile, indent=4, cls = CustomJSONEncoder)

                    # Record the call ID
                    call_ids_file.write(call_id + "\n")
                
            except Exception as e:
                print(f"Error processing call ID {call_id}: {e}")
                continue



def main():
    # Establish database connection
    conn = connect_to_snow_db()

    # Specify the call ID
    call_id = "4bcf3b19-6198-476f-88e2-f5ed23a02854"

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

# Path to the uploaded JSON file
    file_path = '1_25_mentoring_call_ids.json'

# Load the JSON content
    with open(file_path, 'r') as file:
        call_ids = json.load(file)


    process_call_ids(call_ids, conn)

if __name__ == "__main__":
    main()
