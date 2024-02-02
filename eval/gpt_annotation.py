import yaml
import os
import json
from datetime import datetime
import sys

# Assuming langchain and snow_db_utils are installed or available in the parent directory
from langchain.chat_models import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage

# Load YAML configuration
config_path = 'config.yml'  # Ensure this path is correct relative to the script's execution directory
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

# Access configurations
model_config = config['model_config']
file_paths = config['file_paths']
sys_message_file_path = config['sys_message']['file_path']
with open(sys_message_file_path, 'r') as file:
    sys_message_content = file.read().strip()  # Read and strip any trailing newlines

task = config['task']
if task == "DOB":
    from dob_utils import classify_dob, validate_date_format


# Update sys.path to include parent directory for custom modules if necessary
current_script_path = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(current_script_path, "..")
sys.path.insert(0, parent_dir)

# Mocking database connection setup for demonstration
# from snow_db_utils import connect_to_snow_db
# conn = connect_to_snow_db()

# Initialize the model with configuration parameters
model = ChatOpenAI(model=model_config['model_name'], temperature=model_config['temperature'])

def convertTranscript(transcript):
    if not transcript:
        return ""
    conversation_text = ""
    for item in transcript:
        speaker = 'nurse' if item['speaker'] == 'assistant' else 'patient' if item['speaker'] == 'user' else item['speaker']
        conversation_text += f"{speaker}: {item['utterance']}\n"
    return conversation_text

def queryGPT(model, sys_msg, conversation_text):
    response = model([
        SystemMessage(content=sys_msg),
        HumanMessage(content=conversation_text)
    ])
    return response.content


"""
def validate_date_format(dob):
    parts = dob.split('-')
    month, day, year = 'XX', 'XX', 'XXXX'
    if len(parts) == 3:
        month = parts[0] if parts[0].isdigit() else 'XX'
        day = parts[1] if parts[1].isdigit() else 'XX'
        year = parts[2] if parts[2].isdigit() else 'XXXX'
        try:
            date_str = f"{int(month):02d}-{int(day):02d}-{year}"
            datetime.strptime(date_str, "%m-%d-%Y")
            return date_str
        except ValueError:
            return f"{month}-{day}-{year}"
    else:
        return "XX-XX-XXXX"
"""

def transcriptFind(call_id):
    file_path = '../DOB/transcript_DOB_' + call_id + ".json"
    if not os.path.exists(file_path):  # Check if the file exists
        print(f"File not found: {file_path}")  # Optionally log an error or handle it as needed
        return None  # Return None to indicate the file doesn't exist or an error occurred
    with open(file_path, 'r') as file:
        transcript = json.load(file)
    if not transcript or len(transcript) == 0:
        return None  # Return None to indicate an empty transcript
    return transcript

def create_output_entry(call_id, transcript, dob_extracted):
    return {
        "messages": transcript,
        "metadata": {
            "call_id": call_id,  # Replace with actual call ID
            "DOB_extracted": dob_extracted
        }
    }

def process_call_ids():
    call_ids_file = file_paths['call_ids_file']
    with open(call_ids_file, 'r') as file:
        call_ids = json.load(file)

    class_counts = {
        'class_1_count': 0,  # All fields are present
        'class_2_count': 0,  # Partial fields are present
        'class_3_count': 0   # No fields are present (XX-XX-XXXX)
    }

    all_results = []
    for call_id in call_ids:
        if not call_id.strip():
            continue
        transcript = transcriptFind(call_id)
        if transcript is None:
            print(f"Skipping call_id {call_id} due to missing or empty transcript.")
            continue
        conversation_text = convertTranscript(transcript)
        dob_extracted = queryGPT(model, sys_message_content, conversation_text)
        dob_extracted = validate_date_format(dob_extracted)

        classification = classify_dob(dob_extracted)
        if classification == 1:
            class_counts['class_1_count'] += 1
        elif classification == 2:
            class_counts['class_2_count'] += 1
        else:
            class_counts['class_3_count'] += 1

        output_entry = create_output_entry(call_id, transcript, dob_extracted)
        all_results.append(output_entry)
    output_data = {
        "class_counts": class_counts,
        "results": all_results
    }

    output_file = os.path.join(file_paths['output_directory'], "all_DOB_extracted.json")
    with open(output_file, 'w') as file:
        json.dump(output_data, file, indent=4)

if __name__ == "__main__":
    process_call_ids()
