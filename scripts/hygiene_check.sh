#!/usr/bin/env bash
set -eo pipefail

echo "=== Running Callsheet Hygiene Checks ==="

FORBIDDEN_DEPS="strands|boto3|bedrock|anthropic|openai|langchain|crewai|autogen"
EMPLOYER_HYGIENE="REDACTED|REDACTED|REDACTED"

FAIL=0

# Check 1: Forbidden non-Google AI and agent dependencies/imports
echo "Check 1: Prohibited third-party AI frameworks and APIs..."
# Grep excluding this hygiene script itself and .git directory
FORBIDDEN_MATCHES=$(git grep -iE "$FORBIDDEN_DEPS" -- ':(exclude)scripts/hygiene_check.sh' ':(exclude).git' || true)
if [ -n "$FORBIDDEN_MATCHES" ]; then
    echo "ERROR: Found forbidden framework or library references:"
    echo "$FORBIDDEN_MATCHES"
    FAIL=1
else
    echo "PASS: No forbidden framework or library references found."
fi

# Check 2: Employer hygiene keywords
echo "Check 2: Employer hygiene keywords..."
EMPLOYER_MATCHES=$(git grep -iE "$EMPLOYER_HYGIENE" -- ':(exclude)scripts/hygiene_check.sh' ':(exclude).git' || true)
if [ -n "$EMPLOYER_MATCHES" ]; then
    echo "ERROR: Found employer hygiene keywords:"
    echo "$EMPLOYER_MATCHES"
    FAIL=1
else
    echo "PASS: No employer hygiene keywords found."
fi

# Check 3: Git author identity
echo "Check 3: Git author identity..."
if git rev-parse --verify HEAD >/dev/null 2>&1; then
    AUTHORS=$(git log --format='%an <%ae>' | sort -u)
    echo "Commit authors in history:"
    echo "$AUTHORS"
    
    # Ensure all commit authors contain Robert Moore
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

if [ "$FAIL" -ne 0 ]; then
    echo "=== HYGIENE CHECKS FAILED ==="
    exit 1
fi

echo "=== ALL HYGIENE CHECKS PASSED ==="
exit 0
