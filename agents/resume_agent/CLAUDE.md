# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Resume Screening Agent — Nexgensis Technologies

## Project Overview
An AI agent that automates resume screening for job applications at Nexgensis Technologies.
Candidates fill a Google Form, resumes are stored in Google Drive, and the agent evaluates them against a job description and writes results back to the form responses Excel file.

## Setup

```bash
# Create and activate virtualenv
python -m venv venv
source venv/bin/activate

# Install only the packages this project needs (requirements.txt is a full system freeze)
pip install groq pdfplumber pandas openpyxl python-dotenv google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client
```

`.env` must contain:
```
GROQ_API_KEY=your_key_here
```

`credentials.json` must be a Google OAuth2 Desktop app credential downloaded from Google Cloud Console with Drive API enabled.

## Running the Agent

```bash
# Run the main agent (processes up to BATCH_SIZE candidates)
python agents.py
```

On first run, a browser window opens for Google OAuth. `token.json` is saved for subsequent runs.

To change batch size or paths, edit the constants at the bottom of `agents.py`:
```python
BATCH_SIZE = 4   # set to None to process all
```

## Problem Statement
HR was manually reviewing 300-500 resumes per job posting. This agent automates that process.

## Tech Stack
- Python
- Groq API (llama-3.3-70b-versatile) — AI brain for resume evaluation
- pdfplumber — extract text from PDF resumes
- pandas + openpyxl — read/write Excel output
- python-dotenv — manage API key via .env
- Google Drive API (OAuth2) — download resumes submitted via Google Form

## Folder Structure
```
resume_agent/
├── agents.py                          ← main script
├── agent_v2.py                        ← true agentic version (next)
├── job_description.txt                ← paste JD here before running
├── .env                               ← GROQ_API_KEY stored here
├── credentials.json                   ← Google OAuth2 credentials
├── token.json                         ← saved Google auth token (auto-generated)
├── requirements.txt
├── resume/                            ← old local PDF folder (no longer used)
└── output/
    └── Untitled form (Responses).xlsx ← Google Form responses + agent results
```

## How It Works (Current)
1. Candidates fill Google Form — Name, Email, Phone, Upload Resume (Google Drive)
2. HR downloads the form responses Excel into `output/`
3. Run `python agents.py`
4. Agent reads each row, downloads PDF from Drive, evaluates against JD
5. Writes Decision and Reason back into the same Excel file

## Output Format (Excel columns)
| Timestamp | Name | Email | Phone | Upload Resume | Decision | Reason |
|-----------|------|-------|-------|---------------|----------|--------|
| Form submission time | From form | From form | From form | Google Drive link | Selected / Rejected / Maybe | 2-3 sentence explanation |

## Current Job Description
Python + Django Developer (1+ years experience)
- Django & DRF, PostgreSQL, REST APIs, Git
- Work From Office, Pune location

## agents.py — Functions

### `authenticate_drive()` (done)
OAuth2 login to Google Drive. Opens browser on first run, saves `token.json` for future runs.

### `download_resume_from_drive(service, file_id, filename)` (done)
Downloads PDF from Google Drive to `/tmp/` using file ID extracted from the Drive URL.

### `extract_text_from_pdf(pdf_path)` (done)
Extracts all text from a PDF using pdfplumber.

### `load_job_description(jd_path)` (done)
Reads job description from `job_description.txt`.

### `call_groq_with_retry(messages, retries=3, delay=5)` (done)
Calls Groq API with retry logic (3 attempts, 5 second delay).

### `evaluate_resume(resume_text, job_description)` (done)
Sends resume + JD to LLM. Returns Decision (Selected/Rejected/Maybe) and Reason.

### `parse_evaluation(response_text)` (done)
Parses LLM response to extract Decision and Reason fields.

### `process_resumes(excel_path, jd_path, output_path, batch_size=None)` (done)
- Reads candidates from Excel
- Skips rows that already have Decision filled (resume from crash)
- Downloads and evaluates each resume
- Saves progress after each candidate
- Stops after `batch_size` candidates if specified

### Important: Drive URL format assumption
`process_resumes` parses the file ID with `drive_url.split('id=')[1]`. This works only for URLs of the form `https://drive.google.com/open?id=FILE_ID`. If Google Form starts producing `/file/d/FILE_ID/view` style URLs, this parsing will break.

### Removed
- `extract_candidate_info()` — no longer needed, Name/Email/Phone come from Google Form

## Next — agent_v2.py (true agentic version, not yet created)
LLM drives the loop autonomously using tools. You give it a goal, it decides what to call and when.

### Tools the LLM will have:
| Tool | Purpose |
|------|---------|
| `get_unprocessed_candidates(excel_path)` | Returns candidates without Decision |
| `download_resume_from_drive(file_id, name)` | Downloads PDF from Drive |
| `extract_text_from_pdf(pdf_path)` | Extracts text from PDF |
| `evaluate_candidate(resume_text, jd_text)` | Returns Decision + Reason |
| `save_result(excel_path, name, decision, reason)` | Saves result back to Excel |

### What the LLM decides autonomously:
- Which tool to call next
- When the goal is complete
- How to handle errors (retry, skip, stop)
- What to do with each tool result
