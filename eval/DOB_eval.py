import json
from uuid import UUID
from datetime import datetime
import os
import json
import sys

from langchain.chat_models import ChatOpenAI
from langchain.memory import ConversationBufferMemory
from langchain.llms import OpenAI
from langchain.schema import HumanMessage, SystemMessage

current_script_path = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(current_script_path, "..")

sys.path.insert(0, parent_dir)

from snow_db_utils import (
    connect_to_snow_db, 
    get_call_data_from_call_id, 
    get_app_logs_from_call_id, 
    get_transcript_with_votes_from_call_id,
    get_transcript_with_dob_engine
)

conn = connect_to_snow_db()


model = ChatOpenAI(model="gpt-4.0", temperature=0.5)

def convertTranscript(transcript):
    if not transcript:
        return ""
    conversation_text = ""
    for item in transcript:
        speaker = item['speaker']
        if speaker == 'assistant':
            speaker = 'nurse'
        elif speaker == 'user':
            speaker = 'patient'
        conversation_text += f"{speaker}: {item['utterance']}\n"
    return conversation_text


file_path = '../DOB/transcript_DOB_0bf5c7e6-f9dc-4282-84ff-8f7cc08632b8.json'

# Load the JSON content
with open(file_path, 'r') as file:
    transcript = json.load(file)

conversation_text = convertTranscript(transcript)


model=ChatOpenAI(model = "gpt-4-1106-preview", temperature=0.2)
sys_msg = SystemMessage(
        content=(
            f"""
    Please read through a phone conversation occurring between a nurse and a patient. 
    The nurse will verify the patient's date of birth. It may be unclear or given in multiple formats.
    Your task is to extract the date of birth and respond in a 'MM-DD-YYYY' format.
    In cases of uncertainty please fill in what you can and the rest with 'XX'
    For example, if the month and day are difficult to discern, but the year was communicated to be 1975, then output 'XX-XX-1975'
    In cases where it is completely unclear please output 'XX-XX-XXXX'
    """
        )
    )

def queryGPT(model, sys_msg, conversation_text):
    response = model(
            [
                sys_msg,
                HumanMessage(content="\n".join(conversation_text)),
            ]
)
    return response.content


def output_json(call_id, transcript, dob_extracted):

    final_json = {
        "messages": transcript,
        "metadata": {
            "call_id": call_id,  # Replace with actual call ID
            "DOB_extracted": dob_extracted
        }
    }
    file_path = "DOB_output/"+call_id+"_DOB_extracted.json"
    with open(file_path, 'w') as file:
        json.dump(final_json, file, indent=4)

    return

from datetime import datetime

def validate_date_format(dob):
    # Split the input date by '-'
    parts = dob.split('-')
    
    # Initialize default parts to 'XX'
    month, day, year = 'XX', 'XX', 'XXXX'
    
    if len(parts) == 3:
        # Assign parts to month, day, year, substituting 'XX' if part is not a number
        month = parts[0] if parts[0].isdigit() else 'XX'
        day = parts[1] if parts[1].isdigit() else 'XX'
        year = parts[2] if parts[2].isdigit() else 'XXXX'
        
        # Try to create a valid date string from non-'XX' parts
        try:
            date_str = f"{int(month):02d}-{int(day):02d}-{year}"
            # Validate the constructed date string
            datetime.strptime(date_str, "%m-%d-%Y")
            return date_str
        except ValueError:
            # If any part is 'XX' or date is invalid, return the formatted string with 'XX' placeholders
            return f"{month}-{day}-{year}"
    else:
        # Return default format if input doesn't split into three parts
        return "XX-XX-XXXX"

file_path = '../1_25_mentoring_call_ids.json'

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

# Load the JSON content
with open(file_path, 'r') as file:
    call_ids = json.load(file)


all_results=[]
for call_id in call_ids:
    if not call_id.strip():  # Skip if the call_id is blank or only contains whitespace
        continue
    transcript = transcriptFind(call_id)
    if transcript is None:
        print(f"Skipping call_id {call_id} due to missing or empty transcript.")
        continue

    conversation_text = convertTranscript(transcript)
    dob_extracted = queryGPT(model, sys_msg, conversation_text)
    dob_extracted = validate_date_format(dob_extracted)  # Validate the DOB format
    output_entry = create_output_entry(call_id, transcript, dob_extracted)
    all_results.append(output_entry)


file_path = "DOB_output/all_DOB_extracted_og.json"
with open(file_path, 'w') as file:
    json.dump(all_results, file, indent=4)