#!/usr/bin/env bash
set -eo pipefail

echo "=== Running Callsheet Hygiene Checks ==="

FORBIDDEN_DEPS="strands|boto3|bedrock|anthropic|openai|langchain|crewai|autogen"

if [ -n "$HYGIENE_PATTERNS" ]; then
    EMPLOYER_HYGIENE="$HYGIENE_PATTERNS"
elif [ -f ".hygiene_patterns" ]; then
    EMPLOYER_HYGIENE=$(tr -d '\r\n' < .hygiene_patterns)
else
    echo "ERROR: Neither HYGIENE_PATTERNS env var nor .hygiene_patterns file is set."
    exit 1
fi

FAIL=0

# Helper to check a specific set of files
check_pattern() {
    local pattern="$1"
    local desc="$2"
    
    # 1. Check tracked files via git grep
    local tracked_matches
    tracked_matches=$(git grep -iE "$pattern" -- ':(exclude)scripts/hygiene_check.sh' ':(exclude).git' ':(exclude).env*' || true)
    
    # 2. Check untracked non-ignored files
    local untracked_files
    untracked_files=$(git ls-files --others --exclude-standard -x 'scripts/hygiene_check.sh' -x '.env*')
    local untracked_matches=""
    if [ -n "$untracked_files" ]; then
        untracked_matches=$(echo "$untracked_files" | xargs grep -iE "$pattern" 2>/dev/null || true)
    fi
    
    local all_matches=""
    if [ -n "$tracked_matches" ]; then
        all_matches="$tracked_matches"
    fi
    if [ -n "$untracked_matches" ]; then
        if [ -n "$all_matches" ]; then
            all_matches="$all_matches"$'\n'"$untracked_matches"
        else
            all_matches="$untracked_matches"
        fi
    fi
    
    if [ -n "$all_matches" ]; then
        echo "ERROR: Found forbidden matches for $desc:"
        echo "$all_matches"
        FAIL=1
    else
        echo "PASS: No matches found for $desc."
    fi
}

echo "Check 1: Prohibited third-party AI frameworks and APIs..."
check_pattern "$FORBIDDEN_DEPS" "prohibited frameworks"

echo "Check 2: Employer hygiene keywords..."
check_pattern "$EMPLOYER_HYGIENE" "employer hygiene keywords"

echo "Check 3: Git author identity..."
if git rev-parse --verify HEAD >/dev/null 2>&1; then
    AUTHORS=$(git log --format='%an <%ae>' | sort -u)
    echo "Commit authors in history:"
    echo "$AUTHORS"
    
    NON_ROBERT=$(echo "$AUTHORS" | grep -v "Robert Moore" || true)
    if [ -n "$NON_ROBERT" ]; then
        echo "ERROR: Found non-personal author identity:"
        echo "$NON_ROBERT"
        FAIL=1
    else
        echo "PASS: Git history shows only Robert Moore identity."
    fi
else
    echo "PASS: Empty repository, no commits yet."
fi

echo "Check 4: Em-dash hygiene in project source files and documentation..."
EM_DASH_MATCHES=$(git grep -F "—" -- 'callsheet/*' 'README.md' 'JUDGING.md' 'LIMITATIONS.md' 'scripts/*' ':(exclude)scripts/hygiene_check.sh' || true)
if [ -n "$EM_DASH_MATCHES" ]; then
    echo "ERROR: Found forbidden em-dash in project source files:"
    echo "$EM_DASH_MATCHES"
    FAIL=1
else
    echo "PASS: No em-dashes found in project source code or documentation."
fi

echo "Check 5: No direct HTTP calls (urllib.request, requests, httpx) in callsheet/..."
DIRECT_HTTP_MATCHES=$(git grep -iE "(urllib\.request|import requests|from requests|requests\.(get|post|put|patch|delete|request)|import httpx|from httpx|httpx\.(get|post|put|patch|delete|AsyncClient|Client))" -- 'callsheet/*' || true)
if [ -n "$DIRECT_HTTP_MATCHES" ]; then
    echo "ERROR: Found direct HTTP calls in callsheet/ bypassing MCP:"
    echo "$DIRECT_HTTP_MATCHES"
    FAIL=1
else
    echo "PASS: No direct HTTP calls found in callsheet/ (strictly MCP)."
fi

if [ "$FAIL" -ne 0 ]; then
    echo "=== HYGIENE CHECKS FAILED ==="
    exit 1
fi

echo "=== ALL HYGIENE CHECKS PASSED ==="
exit 0
