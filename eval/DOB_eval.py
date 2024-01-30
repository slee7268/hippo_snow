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
    In cases of uncertainty please fill in the 'MM-DD-YYYY' format with 'X'
    In cases where the conversation is blank provide 'XX-XX-XXXX'
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

dob_extracted = queryGPT(model, sys_msg, conversation_text)
call_id = "0bf5c7e6-f9dc-4282-84ff-8f7cc08632b8"
output_json(call_id, transcript, dob_extracted)


file_path = '../1_25_mentoring_call_ids.json'

def transcriptFind(call_id):
    file_path = '../DOB/transcript_DOB_' + call_id +".json"
    #print(file_path)
    with open(file_path, 'r') as file:
        transcript = json.load(file)
    return transcript


# Load the JSON content
with open(file_path, 'r') as file:
    call_ids = json.load(file)


for call_id in call_ids[0:20]:
    if not call_id.strip():  # Skip if the call_id is blank or only contains whitespace
        continue
    transcript = transcriptFind(call_id)
    conversation_text = convertTranscript(transcript)
    dob_extracted = queryGPT(model, sys_msg, conversation_text)
    output_json(call_id, transcript, dob_extracted)

def evaluateConvo():
    return