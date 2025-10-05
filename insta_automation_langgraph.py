import os
from typing import TypedDict, List, Optional
from langgraph.graph import StateGraph, END
from langchain_google_genai import ChatGoogleGenerativeAI
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
import requests
import logging
import time  # Added for retry delays

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Configuration
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME")
INSTAGRAM_ACCOUNT_ID = os.getenv("INSTAGRAM_ACCOUNT_ID")
INSTAGRAM_ACCESS_TOKEN = os.getenv("INSTAGRAM_ACCESS_TOKEN")
FACEBOOK_PAGE_ID = os.getenv("FACEBOOK_PAGE_ID")
FACEBOOK_ACCESS_TOKEN = os.getenv("FACEBOOK_ACCESS_TOKEN")

# Validate that all required environment variables are set
required_vars = [
    "GOOGLE_CREDENTIALS_PATH",
    "GEMINI_API_KEY",
    "SPREADSHEET_ID",
    "SHEET_NAME",
    "INSTAGRAM_ACCOUNT_ID",
    "INSTAGRAM_ACCESS_TOKEN",
    "FACEBOOK_PAGE_ID",
    "FACEBOOK_ACCESS_TOKEN"
]
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")


# Initialize Google Sheets API client
def get_sheets_service():
    logger.info("Initializing Google Sheets API client")
    try:
        creds = Credentials.from_service_account_file(
            GOOGLE_CREDENTIALS_PATH,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        service = build("sheets", "v4", credentials=creds)
        logger.info("Google Sheets API client initialized")
        return service
    except Exception as e:
        logger.error(f"Failed to initialize Google Sheets API client: {str(e)}")
        raise

# State definition for LangGraph
class WorkflowState(TypedDict):
    rows: List[dict]
    current_row_index: int
    caption: Optional[str]
    post_id: Optional[str]
    facebook_post_id: Optional[str]
    error: Optional[str]

# Node 1: Filter rows from Google Sheets
def filter_rows(state: WorkflowState) -> WorkflowState:
    logger.info("Starting filter_rows node")
    try:
        service = get_sheets_service()
        range_name = f"{SHEET_NAME}!A1:DZ"
        logger.info(f"Fetching rows from Google Sheet: {SPREADSHEET_ID}, range: {range_name}")
        
        # Add retry logic for transient errors like 503
        max_retries = 5
        for attempt in range(max_retries):
            try:
                result = service.spreadsheets().values().get(
                    spreadsheetId=SPREADSHEET_ID,
                    range=range_name,
                    valueRenderOption="FORMATTED_VALUE",
                    dateTimeRenderOption="FORMATTED_STRING"
                ).execute()
                break
            except HttpError as e:
                if e.resp.status in [429, 500, 503]:  # Retry on rate limits, internal errors, service unavailable
                    logger.warning(f"Retry attempt {attempt + 1}/{max_retries} after error: {str(e)}")
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise
        else:
            raise Exception("Max retries exceeded for Google Sheets API call")
        
        values = result.get("values", [])
        
        # Filter rows where columns A and B are not empty
        filtered_rows = [
            {"row_number": idx + 2, "prompt": row[0], "image_url": row[1] if len(row) > 1 else ""}
            for idx, row in enumerate(values[1:])  # Skip header
            if len(row) > 1 and row[0] and row[1]
        ]
        
        state["rows"] = filtered_rows
        state["current_row_index"] = 0
        state["error"] = None
        logger.info(f"Filtered {len(filtered_rows)} rows from Google Sheets: {[row['row_number'] for row in filtered_rows]}")
        return state
    except HttpError as e:
        state["error"] = f"Error filtering rows: {str(e)}"
        logger.error(state["error"])
        return state
    except Exception as e:
        state["error"] = f"Error filtering rows: {str(e)}"
        logger.error(state["error"])
        return state
    finally:
        logger.info("Finished filter_rows node")

# Node 2: Generate Instagram caption using Gemini AI
def generate_caption(state: WorkflowState) -> WorkflowState:
    logger.info(f"Starting generate_caption node for row index {state['current_row_index']}")
    if state["error"]:
        logger.warning(f"Skipping generate_caption due to existing error: {state['error']}")
        return state
    
    if state["current_row_index"] >= len(state["rows"]):
        logger.info("No more rows to process in generate_caption")
        state["error"] = "No more rows to process"
        return state
    
    try:
        current_row = state["rows"][state["current_row_index"]]
        logger.info(f"Processing row {current_row['row_number']}: prompt={current_row['prompt'][:50]}...")
        llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", google_api_key=GEMINI_API_KEY)
        prompt = (
            f"Generate a single, concise Instagram post description in English (max 150 words) including trending hashtags "
            f"relevant to the content and brand for an image generated by AI based on the following prompt: "
            f"'{current_row['prompt']}'. The description should be engaging, highlight the key "
            f"elements of the image or product, and align with the brand's aesthetic. Do not provide multiple options; "
            f"provide only one description with a maximum of 150 words, including trending hashtags relevant to the content and brand."
        )
        logger.info("Sending prompt to Gemini AI")
        response = llm.invoke(prompt)
        state["caption"] = response.content.strip()
        state["error"] = None
        logger.info(f"Generated caption for row {current_row['row_number']}: {state['caption'][:50]}...")
        return state
    except Exception as e:
        state["error"] = f"Error generating caption: {str(e)}"
        logger.error(state["error"])
        return state
    finally:
        logger.info("Finished generate_caption node")

# Node 3: Create Instagram post with retry logic
def create_instagram_post(state: WorkflowState) -> WorkflowState:
    logger.info(f"Starting create_instagram_post node for row index {state['current_row_index']}")
    if state["error"]:
        logger.warning(f"Skipping create_instagram_post due to existing error: {state['error']}")
        return state
    
    if state["current_row_index"] >= len(state["rows"]):
        logger.info("No more rows to process in create_instagram_post")
        state["error"] = "No more rows to process"
        return state
    
    try:
        current_row = state["rows"][state["current_row_index"]]
        image_url = current_row["image_url"]
        caption = state["caption"]
        logger.info(f"Attempting to post to Instagram: image_url={image_url}, caption={caption[:50]}...")
        
        # Step 1: Create a media container
        create_media_url = f"https://graph.instagram.com/v21.0/{INSTAGRAM_ACCOUNT_ID}/media"
        payload = {
            "image_url": image_url,
            "caption": caption,
            "access_token": INSTAGRAM_ACCESS_TOKEN
        }
        response = requests.post(create_media_url, data=payload, verify=True)
        response.raise_for_status()
        response_data = response.json()
        media_id = response_data.get("id")
        if not media_id:
            raise ValueError(f"Failed to create media container: {response_data}")
        logger.info(f"Created media container for row {current_row['row_number']}: media_id={media_id}")
        
        # Step 2: Publish the media with retries
        publish_url = f"https://graph.instagram.com/v21.0/{INSTAGRAM_ACCOUNT_ID}/media_publish"
        publish_payload = {
            "creation_id": media_id,
            "access_token": INSTAGRAM_ACCESS_TOKEN
        }
        
        max_retries = 5
        for attempt in range(max_retries):
            publish_response = requests.post(publish_url, data=publish_payload, verify=True)
            if publish_response.status_code == 200:
                publish_data = publish_response.json()
                post_id = publish_data.get("id")
                if post_id:
                    state["post_id"] = post_id
                    state["error"] = None
                    logger.info(f"Posted to Instagram for row {current_row['row_number']}: post_id={post_id}")
                    return state
            else:
                error_json = publish_response.json()
                error_code = error_json.get("error", {}).get("code")
                error_subcode = error_json.get("error", {}).get("error_subcode")
                if error_code == 9007 and error_subcode == 2207027:
                    wait_time = 2 ** attempt
                    logger.warning(f"Media not ready (attempt {attempt+1}/{max_retries}), retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    publish_response.raise_for_status()
        
        raise Exception(f"Failed to publish media after {max_retries} attempts: {publish_response.text}")
    
    except Exception as e:
        state["error"] = f"Error creating Instagram post: {str(e)}"
        logger.error(state["error"])
        return state
    finally:
        logger.info("Finished create_instagram_post node")

# NEW NODE 3b: Create Facebook post (ADDED AFTER INSTAGRAM)
def create_facebook_post(state: WorkflowState) -> WorkflowState:
    logger.info(f"Starting create_facebook_post node for row index {state['current_row_index']}")
    if state["error"]:
        logger.warning(f"Skipping create_facebook_post due to existing error: {state['error']}")
        return state
    
    if state["current_row_index"] >= len(state["rows"]):
        logger.info("No more rows to process in create_facebook_post")
        state["error"] = "No more rows to process"
        return state
    
    try:
        current_row = state["rows"][state["current_row_index"]]
        image_url = current_row["image_url"]
        caption = state["caption"]
        logger.info(f"Attempting to post to Facebook: image_url={image_url}, caption={caption[:50]}...")
        
        # Facebook Graph API endpoint for posting photos to profile
        facebook_url = f"https://graph.facebook.com/v21.0/me/photos"
        payload = {
            "url": image_url,
            "caption": caption,
            "access_token": FACEBOOK_ACCESS_TOKEN  # Your User Token
        }
        
        response = requests.post(facebook_url, data=payload, verify=True)
        response.raise_for_status()
        response_data = response.json()
        
        post_id = response_data.get("id")
        if not post_id:
            raise ValueError(f"Failed to create Facebook post: {response_data}")
        
        state["facebook_post_id"] = post_id
        state["error"] = None
        logger.info(f"Posted to Facebook for row {current_row['row_number']}: post_id={post_id}")
        return state
    except requests.exceptions.HTTPError as e:
        state["error"] = f"Error creating Facebook post: {str(e)}. Response: {e.response.text}"
        logger.error(state["error"])
        return state
    except Exception as e:
        state["error"] = f"Error creating Facebook post: {str(e)}"
        logger.error(state["error"])
        return state
    finally:
        logger.info("Finished create_facebook_post node")

# Node 4: Clear row in Google Sheets
def clear_row(state: WorkflowState) -> WorkflowState:
    logger.info(f"Starting clear_row node for row index {state['current_row_index']}")
    if state["current_row_index"] >= len(state["rows"]):
        logger.info("No more rows to clear in clear_row")
        state["error"] = "No more rows to process"
        return state
    
    clear_success = False
    try:
        current_row = state["rows"][state["current_row_index"]]
        row_number = current_row["row_number"]
        range_name = f"{SHEET_NAME}!A{row_number}:DZ{row_number}"
        logger.info(f"Clearing row {row_number} in Google Sheet: {range_name}")
        service = get_sheets_service()
        
        # Add retry logic for transient errors like 503
        max_retries = 5
        for attempt in range(max_retries):
            try:
                service.spreadsheets().values().clear(
                    spreadsheetId=SPREADSHEET_ID,
                    range=range_name
                ).execute()
                break
            except HttpError as e:
                if e.resp.status in [429, 500, 503]:
                    logger.warning(f"Retry attempt {attempt + 1}/{max_retries} after error: {str(e)}")
                    time.sleep(2 ** attempt)
                else:
                    raise
        else:
            raise Exception("Max retries exceeded for Google Sheets API call")
        
        logger.info(f"Cleared row {row_number}")
        clear_success = True
    except HttpError as e:
        state["error"] = f"Error clearing row: {str(e)}"
        logger.error(state["error"])
        clear_success = False
    except Exception as e:
        state["error"] = f"Error clearing row: {str(e)}"
        logger.error(state["error"])
        clear_success = False
    
    # Always proceed to next row after attempting clear (since posts succeeded)
    state["current_row_index"] += 1
    state["caption"] = None
    state["post_id"] = None
    state["facebook_post_id"] = None
    if not clear_success:
        logger.warning(f"Failed to clear row {current_row['row_number']}, but proceeding to next row to avoid re-processing.")
    state["error"] = None  # Clear any error to allow proceeding
    logger.info(f"Moved to next row index: {state['current_row_index']}")
    return state

# Conditional edge after Instagram post
def decide_after_instagram(state: WorkflowState) -> str:
    if state.get("post_id") and not state.get("error"):
        logger.info("Instagram post successful, proceeding to Facebook post")
        return "create_facebook_post"
    else:
        logger.warning(f"Instagram post failed or error: {state.get('error')}, ending workflow for this row")
        return END

# Conditional edge after Facebook post
def decide_after_facebook(state: WorkflowState) -> str:
    if state.get("facebook_post_id") and not state.get("error"):
        logger.info("Facebook post successful, proceeding to clear row")
        return "clear_row"
    else:
        logger.warning(f"Facebook post failed or error: {state.get('error')}, ending workflow for this row")
        return END

# Conditional edge to handle errors or continue processing
def decide_next_step(state: WorkflowState) -> str:
    logger.info("Starting decide_next_step")
    if state["current_row_index"] >= len(state["rows"]):
        logger.info("No more rows to process, routing to END")
        return END
    logger.info(f"More rows to process, routing to generate_caption for row index {state['current_row_index']}")
    return "generate_caption"

# Build the LangGraph workflow
workflow = StateGraph(WorkflowState)

# Add nodes
workflow.add_node("filter_rows", filter_rows)
workflow.add_node("generate_caption", generate_caption)
workflow.add_node("create_instagram_post", create_instagram_post)
workflow.add_node("create_facebook_post", create_facebook_post)
workflow.add_node("clear_row", clear_row)

# Define edges
workflow.set_entry_point("filter_rows")
workflow.add_edge("filter_rows", "generate_caption")
workflow.add_edge("generate_caption", "create_instagram_post")
workflow.add_conditional_edges(
    "create_instagram_post",
    decide_after_instagram,
    {
        "create_facebook_post": "create_facebook_post",
        END: END
    }
)
workflow.add_conditional_edges(
    "create_facebook_post",
    decide_after_facebook,
    {
        "clear_row": "clear_row",
        END: END
    }
)
workflow.add_conditional_edges(
    "clear_row",
    decide_next_step,
    {
        "generate_caption": "generate_caption",
        END: END
    }
)

# Compile and run the workflow
graph = workflow.compile()

def run_workflow():
    logger.info("Starting workflow execution")
    initial_state = {
        "rows": [],
        "current_row_index": 0,
        "caption": None,
        "post_id": None,  # Instagram
        "facebook_post_id": None,  # Facebook
        "error": None
    }
    # Increase recursion limit to handle large datasets
    for event in graph.stream(initial_state, config={"recursion_limit": 100}):
        logger.info(f"Workflow state update:")

if __name__ == "__main__":
    run_workflow()